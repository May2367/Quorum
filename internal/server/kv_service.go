package server

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"

	"quorum/internal/raft"
	"quorum/internal/statemachine"
	pb "quorum/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type raftRuntime interface {
	Propose(data []byte) (raft.Ready, error)
}

type KVService struct {
	pb.UnimplementedQuorumServer

	mu      sync.Mutex
	raft    raftRuntime
	machine *statemachine.KV
}

func NewKVService(rt raftRuntime, machine *statemachine.KV) *KVService {
	return &KVService{raft: rt, machine: machine}
}

func (s *KVService) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	res, err := s.machine.Get(req.GetKey())
	if err != nil {
		return nil, statusFromStateMachine(err)
	}
	return &pb.GetResponse{Value: res.Value, Revision: res.Revision}, nil
}

func (s *KVService) Put(_ context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	res, err := s.propose(statemachine.Command{
		Type:     statemachine.CmdPut,
		Key:      req.GetKey(),
		Value:    req.GetValue(),
		ClientID: req.GetClientId(),
		Sequence: req.GetSequence(),
	})
	if err != nil {
		return nil, err
	}
	return &pb.PutResponse{Revision: res.Revision}, nil
}

func (s *KVService) Delete(_ context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	res, err := s.propose(statemachine.Command{
		Type:     statemachine.CmdDelete,
		Key:      req.GetKey(),
		ClientID: req.GetClientId(),
		Sequence: req.GetSequence(),
	})
	if err != nil {
		return nil, err
	}
	return &pb.DeleteResponse{Revision: res.Revision}, nil
}

func (s *KVService) CompareAndSwap(_ context.Context, req *pb.CompareAndSwapRequest) (*pb.CompareAndSwapResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	res, err := s.propose(statemachine.Command{
		Type:     statemachine.CmdCAS,
		Key:      req.GetKey(),
		Expect:   req.GetExpected(),
		Value:    req.GetValue(),
		ClientID: req.GetClientId(),
		Sequence: req.GetSequence(),
	})
	if errors.Is(statusError(err), statemachine.ErrCompareFailed) {
		return &pb.CompareAndSwapResponse{Swapped: false, Current: res.Value, Revision: res.Revision}, nil
	}
	if err != nil {
		return nil, err
	}
	return &pb.CompareAndSwapResponse{Swapped: true, Current: res.Value, Revision: res.Revision}, nil
}

func (s *KVService) Watch(req *pb.WatchRequest, stream grpc.ServerStreamingServer[pb.WatchEvent]) error {
	events := s.machine.Watch()
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case ev := <-events:
			if req.GetAfterRevision() > 0 && ev.Revision <= req.GetAfterRevision() {
				continue
			}
			if req.GetPrefix() != "" && !strings.HasPrefix(ev.Key, req.GetPrefix()) {
				continue
			}
			if err := stream.Send(&pb.WatchEvent{
				Key:      ev.Key,
				Value:    ev.Value,
				Revision: ev.Revision,
				Deleted:  ev.Deleted,
			}); err != nil {
				return err
			}
		}
	}
}

func (s *KVService) propose(cmd statemachine.Command) (statemachine.Result, error) {
	payload, err := encodeCommand(cmd)
	if err != nil {
		return statemachine.Result{}, status.Error(codes.Internal, err.Error())
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	rd, err := s.raft.Propose(payload)
	if err != nil {
		return statemachine.Result{}, status.Error(codes.Internal, err.Error())
	}
	var result statemachine.Result
	var applied bool
	for _, ent := range rd.Committed {
		if len(ent.Data) == 0 {
			continue
		}
		committed, err := decodeCommand(ent.Data)
		if err != nil {
			return statemachine.Result{}, status.Error(codes.Internal, err.Error())
		}
		committed.LogIndex = ent.Index
		res, applyErr := s.machine.Apply(committed)
		if sameCommand(committed, cmd) {
			result = res
			applied = true
			if applyErr != nil {
				return result, statusFromStateMachine(applyErr)
			}
		} else if applyErr != nil && !errors.Is(applyErr, statemachine.ErrDuplicateLower) {
			return statemachine.Result{}, statusFromStateMachine(applyErr)
		}
	}
	if !applied {
		return statemachine.Result{}, status.Error(codes.Unavailable, "proposal was not committed locally")
	}
	return result, nil
}

func encodeCommand(cmd statemachine.Command) ([]byte, error) {
	return json.Marshal(cmd)
}

func decodeCommand(data []byte) (statemachine.Command, error) {
	var cmd statemachine.Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return statemachine.Command{}, err
	}
	return cmd, nil
}

func sameCommand(a, b statemachine.Command) bool {
	return a.Type == b.Type &&
		a.Key == b.Key &&
		a.Value == b.Value &&
		a.Expect == b.Expect &&
		a.ClientID == b.ClientID &&
		a.Sequence == b.Sequence
}

func statusFromStateMachine(err error) error {
	switch {
	case errors.Is(err, statemachine.ErrKeyNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, statemachine.ErrCompareFailed):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, statemachine.ErrDuplicateLower):
		return status.Error(codes.AlreadyExists, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func statusError(err error) error {
	if err == nil {
		return nil
	}
	if status.Code(err) == codes.FailedPrecondition {
		return statemachine.ErrCompareFailed
	}
	return err
}
