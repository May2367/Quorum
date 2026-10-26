package server

import (
	"bytes"
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
	Apply            func([]raft.Entry)
}

type AsyncRaft struct {
	rt          *PersistentRaft
	requests    chan asyncRequest
	done        chan struct{}
	closed      chan struct{}
	closeOnce   sync.Once
	sender      ReadyMessageSender
	sendTimeout time.Duration
	apply       func([]raft.Entry)
	waiters     []*proposalWaiter
}

type proposalWaiter struct {
	data []byte
	resp chan asyncResult
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
	asyncProposeAndWait
	asyncStatus
)

type asyncResult struct {
	ready  raft.Ready
	status RaftStatus
	err    error
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
		apply:       opts.Apply,
	}
	go driver.run()
	return driver, nil
}

func (d *AsyncRaft) AppliesCommittedEntries() bool {
	return d.apply != nil
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

func (d *AsyncRaft) ProposeAndWait(ctx context.Context, data []byte) (raft.Ready, error) {
	return d.callContext(ctx, asyncRequest{kind: asyncProposeAndWait, data: append([]byte(nil), data...)})
}

func (d *AsyncRaft) Status() (RaftStatus, error) {
	res, err := d.callResult(context.Background(), asyncRequest{kind: asyncStatus})
	return res.status, err
}

func (d *AsyncRaft) Close() error {
	d.closeOnce.Do(func() {
		close(d.done)
		<-d.closed
	})
	return nil
}

func (d *AsyncRaft) call(req asyncRequest) (raft.Ready, error) {
	return d.callContext(context.Background(), req)
}

func (d *AsyncRaft) callContext(ctx context.Context, req asyncRequest) (raft.Ready, error) {
	res, err := d.callResult(ctx, req)
	return res.ready, err
}

func (d *AsyncRaft) callResult(ctx context.Context, req asyncRequest) (asyncResult, error) {
	req.resp = make(chan asyncResult, 1)
	select {
	case d.requests <- req:
	case <-d.done:
		return asyncResult{}, ErrAsyncRaftClosed
	case <-ctx.Done():
		return asyncResult{}, ctx.Err()
	}
	select {
	case res := <-req.resp:
		return res, res.err
	case <-d.done:
		return asyncResult{}, ErrAsyncRaftClosed
	case <-ctx.Done():
		return asyncResult{}, ctx.Err()
	}
}

func (d *AsyncRaft) run() {
	defer close(d.closed)
	defer d.rt.Close()
	defer d.failWaiters(ErrAsyncRaftClosed)

	for {
		select {
		case <-d.done:
			return
		case req := <-d.requests:
			d.handleRequest(req)
		}
	}
}

func (d *AsyncRaft) handleRequest(req asyncRequest) {
	if req.kind == asyncStatus {
		status, err := d.rt.Status()
		req.resp <- asyncResult{status: status, err: err}
		return
	}
	if req.kind == asyncProposeAndWait {
		d.handleProposeAndWait(req)
		return
	}
	rd, err := d.handle(req)
	if err == nil {
		d.afterReady(rd)
	}
	req.resp <- asyncResult{ready: rd, err: err}
}

func (d *AsyncRaft) handleProposeAndWait(req asyncRequest) {
	waiter := &proposalWaiter{data: append([]byte(nil), req.data...), resp: req.resp}
	d.waiters = append(d.waiters, waiter)
	rd, err := d.handle(asyncRequest{kind: asyncPropose, data: req.data})
	if err != nil {
		d.removeWaiter(waiter)
		req.resp <- asyncResult{err: err}
		return
	}
	d.afterReady(rd)
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

func (d *AsyncRaft) afterReady(rd raft.Ready) {
	d.completeWaiters(rd)
	d.applyCommitted(rd.Committed)
	d.dispatch(rd.Messages)
}

func (d *AsyncRaft) applyCommitted(entries []raft.Entry) {
	if d.apply == nil || len(entries) == 0 {
		return
	}
	d.apply(entries)
}

func (d *AsyncRaft) completeWaiters(rd raft.Ready) {
	if len(d.waiters) == 0 || len(rd.Committed) == 0 {
		return
	}
	remaining := d.waiters[:0]
	for _, waiter := range d.waiters {
		if readyHasData(rd, waiter.data) {
			waiter.resp <- asyncResult{ready: rd}
			continue
		}
		remaining = append(remaining, waiter)
	}
	d.waiters = remaining
}

func readyHasData(rd raft.Ready, data []byte) bool {
	for _, ent := range rd.Committed {
		if bytes.Equal(ent.Data, data) {
			return true
		}
	}
	return false
}

func (d *AsyncRaft) removeWaiter(target *proposalWaiter) bool {
	removed := false
	remaining := d.waiters[:0]
	for _, waiter := range d.waiters {
		if waiter == target {
			removed = true
			continue
		}
		remaining = append(remaining, waiter)
	}
	d.waiters = remaining
	return removed
}

func (d *AsyncRaft) failWaiters(err error) {
	for _, waiter := range d.waiters {
		waiter.resp <- asyncResult{err: err}
	}
	d.waiters = nil
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
