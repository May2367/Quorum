package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

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

type waitingRaftRuntime interface {
	ProposeAndWait(context.Context, []byte) (raft.Ready, error)
}

type applyingRaftRuntime interface {
	AppliesCommittedEntries() bool
}

type statusRaftRuntime interface {
	Status() (RaftStatus, error)
}

type applyResult struct {
	result statemachine.Result
	err    error
}

type KVService struct {
	pb.UnimplementedQuorumServer

	mu        sync.Mutex
	raft      raftRuntime
	machine   *statemachine.KV
	serviceID uint64
	nextID    uint64
	waiters   map[string]chan applyResult
}

var nextKVServiceID atomic.Uint64

func NewKVService(rt raftRuntime, machine *statemachine.KV) *KVService {
	return &KVService{
		raft:      rt,
		machine:   machine,
		serviceID: nextKVServiceID.Add(1),
		waiters:   map[string]chan applyResult{},
	}
}

func (s *KVService) SetRaftRuntime(rt raftRuntime) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.raft = rt
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

func (s *KVService) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	res, err := s.propose(ctx, statemachine.Command{
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

func (s *KVService) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	res, err := s.propose(ctx, statemachine.Command{
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

func (s *KVService) CompareAndSwap(ctx context.Context, req *pb.CompareAndSwapRequest) (*pb.CompareAndSwapResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	res, err := s.propose(ctx, statemachine.Command{
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

func (s *KVService) propose(ctx context.Context, cmd statemachine.Command) (statemachine.Result, error) {
	if s.usesAsyncApply() {
		return s.proposeAndWaitApplied(ctx, cmd)
	}
	return s.proposeAndApplyReady(ctx, cmd)
}

func (s *KVService) proposeAndWaitApplied(ctx context.Context, cmd statemachine.Command) (statemachine.Result, error) {
	if err := s.ensureLeader(); err != nil {
		return statemachine.Result{}, err
	}
	cmd.ProposalID = s.nextProposalID()
	payload, err := encodeCommand(cmd)
	if err != nil {
		return statemachine.Result{}, status.Error(codes.Internal, err.Error())
	}
	waiter := s.registerWaiter(cmd.ProposalID)
	if _, err := s.raft.Propose(payload); err != nil {
		s.removeWaiter(cmd.ProposalID)
		return statemachine.Result{}, status.Error(codes.Internal, err.Error())
	}
	select {
	case res := <-waiter:
		if res.err != nil {
			return res.result, statusFromStateMachine(res.err)
		}
		return res.result, nil
	case <-ctx.Done():
		s.removeWaiter(cmd.ProposalID)
		return statemachine.Result{}, status.Error(codes.DeadlineExceeded, ctx.Err().Error())
	}
}

func (s *KVService) proposeAndApplyReady(ctx context.Context, cmd statemachine.Command) (statemachine.Result, error) {
	cmd.ProposalID = s.nextProposalID()
	payload, err := encodeCommand(cmd)
	if err != nil {
		return statemachine.Result{}, status.Error(codes.Internal, err.Error())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureLeaderLocked(); err != nil {
		return statemachine.Result{}, err
	}
	rd, err := s.proposeRaft(ctx, payload)
	if err != nil {
		return statemachine.Result{}, status.Error(codes.Internal, err.Error())
	}
	return s.applyReadyForCommandLocked(rd, cmd)
}

func (s *KVService) ApplyCommitted(entries []raft.Entry) {
	if len(entries) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ent := range entries {
		s.applyEntryLocked(ent, nil)
	}
}

func (s *KVService) applyReadyForCommandLocked(rd raft.Ready, cmd statemachine.Command) (statemachine.Result, error) {
	var result statemachine.Result
	var applied bool
	for _, ent := range rd.Committed {
		res, applyErr, matched, err := s.applyEntryLocked(ent, &cmd)
		if err != nil {
			return statemachine.Result{}, status.Error(codes.Internal, err.Error())
		}
		if matched {
			result = res
			applied = true
			if applyErr != nil {
				return result, statusFromStateMachine(applyErr)
			}
		}
	}
	if !applied {
		return statemachine.Result{}, status.Error(codes.Unavailable, "proposal was not committed locally")
	}
	return result, nil
}

func (s *KVService) applyEntryLocked(ent raft.Entry, target *statemachine.Command) (statemachine.Result, error, bool, error) {
	if len(ent.Data) == 0 {
		return statemachine.Result{}, nil, false, nil
	}
	committed, err := decodeCommand(ent.Data)
	if err != nil {
		return statemachine.Result{}, nil, false, err
	}
	committed.LogIndex = ent.Index
	res, applyErr := s.machine.Apply(committed)
	if committed.ProposalID != "" {
		s.completeWaiterLocked(committed.ProposalID, applyResult{result: res, err: applyErr})
	}
	matched := target != nil && sameCommand(committed, *target)
	return res, applyErr, matched, nil
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
		a.Sequence == b.Sequence &&
		a.ProposalID == b.ProposalID
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

func (s *KVService) proposeRaft(ctx context.Context, payload []byte) (raft.Ready, error) {
	if waiter, ok := s.raft.(waitingRaftRuntime); ok {
		return waiter.ProposeAndWait(ctx, payload)
	}
	return s.raft.Propose(payload)
}

func (s *KVService) usesAsyncApply() bool {
	runtime, ok := s.raft.(applyingRaftRuntime)
	return ok && runtime.AppliesCommittedEntries()
}

func (s *KVService) nextProposalID() string {
	next := atomic.AddUint64(&s.nextID, 1)
	return fmt.Sprintf("kv-%d-%d", s.serviceID, next)
}

func (s *KVService) registerWaiter(proposalID string) <-chan applyResult {
	ch := make(chan applyResult, 1)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.waiters[proposalID] = ch
	return ch
}

func (s *KVService) removeWaiter(proposalID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.waiters, proposalID)
}

func (s *KVService) completeWaiterLocked(proposalID string, res applyResult) {
	ch, ok := s.waiters[proposalID]
	if !ok {
		return
	}
	delete(s.waiters, proposalID)
	ch <- res
}

func (s *KVService) ensureLeader() error {
	runtime, ok := s.raft.(statusRaftRuntime)
	if !ok {
		return nil
	}
	st, err := runtime.Status()
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	if !st.IsLeader() {
		return notLeaderError(st)
	}
	return nil
}

func (s *KVService) ensureLeaderLocked() error {
	runtime, ok := s.raft.(statusRaftRuntime)
	if !ok {
		return nil
	}
	st, err := runtime.Status()
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	if !st.IsLeader() {
		return notLeaderError(st)
	}
	return nil
}

func notLeaderError(st RaftStatus) error {
	if st.Leader == 0 {
		return status.Error(codes.Unavailable, "not leader: leader_id=0")
	}
	return status.Error(codes.Unavailable, fmt.Sprintf("not leader: leader_id=%d", st.Leader))
}
