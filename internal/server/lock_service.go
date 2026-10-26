package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"quorum/internal/raft"
	"quorum/internal/statemachine"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type LockCommandType uint8

const (
	LockCmdAcquire LockCommandType = iota
	LockCmdKeepAlive
	LockCmdRelease
	LockCmdAdvance
)

type LockCommand struct {
	Type       LockCommandType
	Name       string
	Holder     string
	TTLTicks   uint64
	Ticks      uint64
	ProposalID string
	LogIndex   uint64
}

type lockApplyResult struct {
	result statemachine.LockResult
	err    error
}

type LockService struct {
	mu        sync.Mutex
	raft      raftRuntime
	locks     *statemachine.LockTable
	serviceID uint64
	nextID    uint64
	waiters   map[string]chan lockApplyResult
}

var nextLockServiceID atomic.Uint64

func NewLockService(rt raftRuntime, locks *statemachine.LockTable) *LockService {
	return &LockService{
		raft:      rt,
		locks:     locks,
		serviceID: nextLockServiceID.Add(1),
		waiters:   map[string]chan lockApplyResult{},
	}
}

func (s *LockService) SetRaftRuntime(rt raftRuntime) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.raft = rt
}

func (s *LockService) Acquire(ctx context.Context, name, holder string, ttlTicks uint64) (statemachine.LockResult, error) {
	if name == "" || holder == "" {
		return statemachine.LockResult{}, status.Error(codes.InvalidArgument, "lock name and holder are required")
	}
	return s.propose(ctx, LockCommand{Type: LockCmdAcquire, Name: name, Holder: holder, TTLTicks: ttlTicks})
}

func (s *LockService) KeepAlive(ctx context.Context, name, holder string, ttlTicks uint64) (statemachine.LockResult, error) {
	if name == "" || holder == "" {
		return statemachine.LockResult{}, status.Error(codes.InvalidArgument, "lock name and holder are required")
	}
	return s.propose(ctx, LockCommand{Type: LockCmdKeepAlive, Name: name, Holder: holder, TTLTicks: ttlTicks})
}

func (s *LockService) Release(ctx context.Context, name, holder string) error {
	if name == "" || holder == "" {
		return status.Error(codes.InvalidArgument, "lock name and holder are required")
	}
	_, err := s.propose(ctx, LockCommand{Type: LockCmdRelease, Name: name, Holder: holder})
	return err
}

func (s *LockService) Advance(ctx context.Context, ticks uint64) error {
	_, err := s.propose(ctx, LockCommand{Type: LockCmdAdvance, Ticks: ticks})
	return err
}

func (s *LockService) ApplyCommitted(entries []raft.Entry) {
	if len(entries) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ent := range entries {
		s.applyEntryLocked(ent, nil)
	}
}

func (s *LockService) propose(ctx context.Context, cmd LockCommand) (statemachine.LockResult, error) {
	if err := s.ensureLeader(); err != nil {
		return statemachine.LockResult{}, err
	}
	cmd.ProposalID = s.nextProposalID()
	payload, err := encodeLockCommand(cmd)
	if err != nil {
		return statemachine.LockResult{}, status.Error(codes.Internal, err.Error())
	}
	if s.usesAsyncApply() {
		waiter := s.registerWaiter(cmd.ProposalID)
		if _, err := s.raft.Propose(payload); err != nil {
			s.removeWaiter(cmd.ProposalID)
			return statemachine.LockResult{}, status.Error(codes.Internal, err.Error())
		}
		select {
		case res := <-waiter:
			if res.err != nil {
				return res.result, statusFromLock(res.err)
			}
			return res.result, nil
		case <-ctx.Done():
			s.removeWaiter(cmd.ProposalID)
			return statemachine.LockResult{}, status.Error(codes.DeadlineExceeded, ctx.Err().Error())
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	rd, err := s.proposeRaft(ctx, payload)
	if err != nil {
		return statemachine.LockResult{}, status.Error(codes.Internal, err.Error())
	}
	return s.applyReadyForCommandLocked(rd, cmd)
}

func (s *LockService) applyReadyForCommandLocked(rd raft.Ready, cmd LockCommand) (statemachine.LockResult, error) {
	var result statemachine.LockResult
	var applied bool
	for _, ent := range rd.Committed {
		res, applyErr, matched, err := s.applyEntryLocked(ent, &cmd)
		if err != nil {
			return statemachine.LockResult{}, status.Error(codes.Internal, err.Error())
		}
		if matched {
			result = res
			applied = true
			if applyErr != nil {
				return result, statusFromLock(applyErr)
			}
		}
	}
	if !applied {
		return statemachine.LockResult{}, status.Error(codes.Unavailable, "proposal was not committed locally")
	}
	return result, nil
}

func (s *LockService) applyEntryLocked(ent raft.Entry, target *LockCommand) (statemachine.LockResult, error, bool, error) {
	if len(ent.Data) == 0 {
		return statemachine.LockResult{}, nil, false, nil
	}
	cmd, err := decodeLockCommand(ent.Data)
	if errors.Is(err, errForeignCommand) {
		return statemachine.LockResult{}, nil, false, nil
	}
	if err != nil {
		return statemachine.LockResult{}, nil, false, err
	}
	cmd.LogIndex = ent.Index
	res, applyErr := s.applyCommandLocked(cmd)
	if cmd.ProposalID != "" {
		s.completeWaiterLocked(cmd.ProposalID, lockApplyResult{result: res, err: applyErr})
	}
	matched := target != nil && sameLockCommand(cmd, *target)
	return res, applyErr, matched, nil
}

func (s *LockService) applyCommandLocked(cmd LockCommand) (statemachine.LockResult, error) {
	switch cmd.Type {
	case LockCmdAcquire:
		return s.locks.Acquire(cmd.Name, cmd.Holder, cmd.TTLTicks, cmd.LogIndex)
	case LockCmdKeepAlive:
		return s.locks.KeepAlive(cmd.Name, cmd.Holder, cmd.TTLTicks)
	case LockCmdRelease:
		return statemachine.LockResult{Name: cmd.Name, Holder: cmd.Holder}, s.locks.Release(cmd.Name, cmd.Holder)
	case LockCmdAdvance:
		s.locks.Advance(cmd.Ticks)
		return statemachine.LockResult{ExpiresAt: s.locks.Tick(), Acquired: true}, nil
	default:
		return statemachine.LockResult{}, errors.New("unknown lock command type")
	}
}

func encodeLockCommand(cmd LockCommand) ([]byte, error) {
	return encodeServiceCommand("lock", cmd)
}

func decodeLockCommand(data []byte) (LockCommand, error) {
	var cmd LockCommand
	if enveloped, err := decodeServiceCommand(data, "lock", &cmd); enveloped {
		return cmd, err
	}
	if err := json.Unmarshal(data, &cmd); err != nil {
		return LockCommand{}, err
	}
	if cmd.Type != LockCmdAdvance && (cmd.Name == "" || cmd.Holder == "") {
		return LockCommand{}, errForeignCommand
	}
	return cmd, nil
}

func sameLockCommand(a, b LockCommand) bool {
	return a.Type == b.Type &&
		a.Name == b.Name &&
		a.Holder == b.Holder &&
		a.TTLTicks == b.TTLTicks &&
		a.Ticks == b.Ticks &&
		a.ProposalID == b.ProposalID
}

func (s *LockService) proposeRaft(ctx context.Context, payload []byte) (raft.Ready, error) {
	if waiter, ok := s.raft.(waitingRaftRuntime); ok {
		return waiter.ProposeAndWait(ctx, payload)
	}
	return s.raft.Propose(payload)
}

func (s *LockService) usesAsyncApply() bool {
	runtime, ok := s.raft.(applyingRaftRuntime)
	return ok && runtime.AppliesCommittedEntries()
}

func (s *LockService) ensureLeader() error {
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

func (s *LockService) nextProposalID() string {
	next := atomic.AddUint64(&s.nextID, 1)
	return fmt.Sprintf("lock-%d-%d", s.serviceID, next)
}

func (s *LockService) registerWaiter(proposalID string) <-chan lockApplyResult {
	ch := make(chan lockApplyResult, 1)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.waiters[proposalID] = ch
	return ch
}

func (s *LockService) removeWaiter(proposalID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.waiters, proposalID)
}

func (s *LockService) completeWaiterLocked(proposalID string, res lockApplyResult) {
	ch, ok := s.waiters[proposalID]
	if !ok {
		return
	}
	delete(s.waiters, proposalID)
	ch <- res
}

func statusFromLock(err error) error {
	switch {
	case errors.Is(err, statemachine.ErrLockHeld),
		errors.Is(err, statemachine.ErrLockNotHeld),
		errors.Is(err, statemachine.ErrLockNotOwner),
		errors.Is(err, statemachine.ErrStaleFencingToken):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, statemachine.ErrInvalidLeaseTTL), errors.Is(err, statemachine.ErrInvalidFence):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
