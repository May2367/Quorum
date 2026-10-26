package server

import (
	"context"
	"errors"
	"sync"
	"time"

	"quorum/internal/raft"
)

var ErrAsyncRaftClosed = errors.New("async raft driver is closed")

type ReadyMessageSender interface {
	Send(context.Context, raft.Message) ([]raft.Message, error)
}

type AsyncRaftOptions struct {
	Sender           ReadyMessageSender
	SendTimeout      time.Duration
	ElectionTimeout  int
	HeartbeatTimeout int
}

type AsyncRaft struct {
	rt          *PersistentRaft
	requests    chan asyncRequest
	done        chan struct{}
	closed      chan struct{}
	closeOnce   sync.Once
	sender      ReadyMessageSender
	sendTimeout time.Duration
}

type asyncRequest struct {
	kind asyncRequestKind
	msg  raft.Message
	data []byte
	resp chan asyncResult
}

type asyncRequestKind uint8

const (
	asyncTick asyncRequestKind = iota
	asyncStep
	asyncPropose
)

type asyncResult struct {
	ready raft.Ready
	err   error
}

func OpenAsyncRaft(dir string, id uint64, peers []uint64, opts AsyncRaftOptions) (*AsyncRaft, error) {
	rt, err := OpenPersistentRaft(dir, id, peers)
	if err != nil {
		return nil, err
	}
	if opts.ElectionTimeout > 0 {
		rt.Node().SetElectionTimeout(opts.ElectionTimeout)
	}
	if opts.HeartbeatTimeout > 0 {
		rt.Node().SetHeartbeatTimeout(opts.HeartbeatTimeout)
	}
	sendTimeout := opts.SendTimeout
	if sendTimeout == 0 {
		sendTimeout = 5 * time.Second
	}
	driver := &AsyncRaft{
		rt:          rt,
		requests:    make(chan asyncRequest),
		done:        make(chan struct{}),
		closed:      make(chan struct{}),
		sender:      opts.Sender,
		sendTimeout: sendTimeout,
	}
	go driver.run()
	return driver, nil
}

func (d *AsyncRaft) Tick() (raft.Ready, error) {
	return d.call(asyncRequest{kind: asyncTick})
}

func (d *AsyncRaft) Step(msg raft.Message) (raft.Ready, error) {
	return d.call(asyncRequest{kind: asyncStep, msg: msg})
}

func (d *AsyncRaft) Propose(data []byte) (raft.Ready, error) {
	return d.call(asyncRequest{kind: asyncPropose, data: append([]byte(nil), data...)})
}

func (d *AsyncRaft) Close() error {
	d.closeOnce.Do(func() {
		close(d.done)
		<-d.closed
	})
	return nil
}

func (d *AsyncRaft) call(req asyncRequest) (raft.Ready, error) {
	req.resp = make(chan asyncResult, 1)
	select {
	case d.requests <- req:
	case <-d.done:
		return raft.Ready{}, ErrAsyncRaftClosed
	}
	select {
	case res := <-req.resp:
		return res.ready, res.err
	case <-d.done:
		return raft.Ready{}, ErrAsyncRaftClosed
	}
}

func (d *AsyncRaft) run() {
	defer close(d.closed)
	defer d.rt.Close()

	for {
		select {
		case <-d.done:
			return
		case req := <-d.requests:
			rd, err := d.handle(req)
			if err == nil {
				d.dispatch(rd.Messages)
			}
			req.resp <- asyncResult{ready: rd, err: err}
		}
	}
}

func (d *AsyncRaft) handle(req asyncRequest) (raft.Ready, error) {
	switch req.kind {
	case asyncTick:
		return d.rt.Tick()
	case asyncStep:
		return d.rt.Step(req.msg)
	case asyncPropose:
		return d.rt.Propose(req.data)
	default:
		return raft.Ready{}, errors.New("unknown async raft request")
	}
}

func (d *AsyncRaft) dispatch(messages []raft.Message) {
	if d.sender == nil {
		return
	}
	for _, msg := range messages {
		msg := msg
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), d.sendTimeout)
			defer cancel()
			responses, err := d.sender.Send(ctx, msg)
			if err != nil {
				return
			}
			for _, response := range responses {
				_, _ = d.Step(response)
			}
		}()
	}
}
