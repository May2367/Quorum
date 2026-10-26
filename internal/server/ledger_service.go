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

type LedgerCommandType uint8

const (
	LedgerCmdCreateAccount LedgerCommandType = iota
	LedgerCmdTransfer
	LedgerCmdBarrier
)

type LedgerCommand struct {
	Type           LedgerCommandType
	AccountID      string
	InitialCents   int64
	FromAccount    string
	ToAccount      string
	Cents          int64
	IdempotencyKey string
	FencingToken   uint64
	ProposalID     string
	LogIndex       uint64
}

type LedgerResult struct {
	Transfer statemachine.TransferResult
	Balance  int64
	Revision uint64
	Applied  bool
}

type ledgerApplyResult struct {
	result LedgerResult
	err    error
}

type LedgerService struct {
	mu        sync.Mutex
	raft      raftRuntime
	ledger    *statemachine.Ledger
	serviceID uint64
	nextID    uint64
	waiters   map[string]chan ledgerApplyResult
}

var nextLedgerServiceID atomic.Uint64

func NewLedgerService(rt raftRuntime, ledger *statemachine.Ledger) *LedgerService {
	return &LedgerService{
		raft:      rt,
		ledger:    ledger,
		serviceID: nextLedgerServiceID.Add(1),
		waiters:   map[string]chan ledgerApplyResult{},
	}
}

func (s *LedgerService) SetRaftRuntime(rt raftRuntime) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.raft = rt
}

func (s *LedgerService) CreateAccount(ctx context.Context, accountID string, initialCents int64) error {
	if accountID == "" {
		return status.Error(codes.InvalidArgument, "account id is required")
	}
	_, err := s.propose(ctx, LedgerCommand{Type: LedgerCmdCreateAccount, AccountID: accountID, InitialCents: initialCents})
	return err
}

func (s *LedgerService) Transfer(ctx context.Context, from, to string, cents int64, idempotencyKey string, fencingToken uint64) (statemachine.TransferResult, error) {
	if from == "" || to == "" {
		return statemachine.TransferResult{}, status.Error(codes.InvalidArgument, "from and to accounts are required")
	}
	res, err := s.propose(ctx, LedgerCommand{Type: LedgerCmdTransfer, FromAccount: from, ToAccount: to, Cents: cents, IdempotencyKey: idempotencyKey, FencingToken: fencingToken})
	return res.Transfer, err
}

func (s *LedgerService) GetBalance(ctx context.Context, accountID string) (int64, error) {
	if accountID == "" {
		return 0, status.Error(codes.InvalidArgument, "account id is required")
	}
	if _, err := s.propose(ctx, LedgerCommand{Type: LedgerCmdBarrier}); err != nil {
		return 0, err
	}
	balance, err := s.ledger.GetBalance(accountID)
	if err != nil {
		return 0, statusFromLedger(err)
	}
	return balance, nil
}

func (s *LedgerService) ApplyCommitted(entries []raft.Entry) {
	if len(entries) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ent := range entries {
		s.applyEntryLocked(ent, nil)
	}
}

func (s *LedgerService) propose(ctx context.Context, cmd LedgerCommand) (LedgerResult, error) {
	if err := s.ensureLeader(); err != nil {
		return LedgerResult{}, err
	}
	cmd.ProposalID = s.nextProposalID()
	payload, err := encodeLedgerCommand(cmd)
	if err != nil {
		return LedgerResult{}, status.Error(codes.Internal, err.Error())
	}
	if s.usesAsyncApply() {
		waiter := s.registerWaiter(cmd.ProposalID)
		if _, err := s.raft.Propose(payload); err != nil {
			s.removeWaiter(cmd.ProposalID)
			return LedgerResult{}, status.Error(codes.Internal, err.Error())
		}
		select {
		case res := <-waiter:
			if res.err != nil {
				return res.result, statusFromLedger(res.err)
			}
			return res.result, nil
		case <-ctx.Done():
			s.removeWaiter(cmd.ProposalID)
			return LedgerResult{}, status.Error(codes.DeadlineExceeded, ctx.Err().Error())
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	rd, err := s.proposeRaft(ctx, payload)
	if err != nil {
		return LedgerResult{}, status.Error(codes.Internal, err.Error())
	}
	return s.applyReadyForCommandLocked(rd, cmd)
}

func (s *LedgerService) applyReadyForCommandLocked(rd raft.Ready, cmd LedgerCommand) (LedgerResult, error) {
	var result LedgerResult
	var applied bool
	for _, ent := range rd.Committed {
		res, applyErr, matched, err := s.applyEntryLocked(ent, &cmd)
		if err != nil {
			return LedgerResult{}, status.Error(codes.Internal, err.Error())
		}
		if matched {
			result = res
			applied = true
			if applyErr != nil {
				return result, statusFromLedger(applyErr)
			}
		}
	}
	if !applied {
		return LedgerResult{}, status.Error(codes.Unavailable, "proposal was not committed locally")
	}
	return result, nil
}

func (s *LedgerService) applyEntryLocked(ent raft.Entry, target *LedgerCommand) (LedgerResult, error, bool, error) {
	if len(ent.Data) == 0 {
		return LedgerResult{}, nil, false, nil
	}
	cmd, err := decodeLedgerCommand(ent.Data)
	if errors.Is(err, errForeignCommand) {
		return LedgerResult{}, nil, false, nil
	}
	if err != nil {
		return LedgerResult{}, nil, false, err
	}
	cmd.LogIndex = ent.Index
	res, applyErr := s.applyCommandLocked(cmd)
	if cmd.ProposalID != "" {
		s.completeWaiterLocked(cmd.ProposalID, ledgerApplyResult{result: res, err: applyErr})
	}
	matched := target != nil && sameLedgerCommand(cmd, *target)
	return res, applyErr, matched, nil
}

func (s *LedgerService) applyCommandLocked(cmd LedgerCommand) (LedgerResult, error) {
	switch cmd.Type {
	case LedgerCmdCreateAccount:
		return LedgerResult{Applied: true}, s.ledger.CreateAccount(cmd.AccountID, cmd.InitialCents)
	case LedgerCmdTransfer:
		res, err := s.ledger.Transfer(cmd.FromAccount, cmd.ToAccount, cmd.Cents, cmd.IdempotencyKey, cmd.FencingToken)
		return LedgerResult{Transfer: res, Revision: res.Revision, Applied: res.Applied}, err
	case LedgerCmdBarrier:
		return LedgerResult{Applied: true}, nil
	default:
		return LedgerResult{}, errors.New("unknown ledger command type")
	}
}

func encodeLedgerCommand(cmd LedgerCommand) ([]byte, error) {
	return encodeServiceCommand("ledger", cmd)
}

func decodeLedgerCommand(data []byte) (LedgerCommand, error) {
	var cmd LedgerCommand
	if enveloped, err := decodeServiceCommand(data, "ledger", &cmd); enveloped {
		return cmd, err
	}
	if err := json.Unmarshal(data, &cmd); err != nil {
		return LedgerCommand{}, err
	}
	switch cmd.Type {
	case LedgerCmdCreateAccount:
		if cmd.AccountID == "" {
			return LedgerCommand{}, errForeignCommand
		}
	case LedgerCmdTransfer:
		if cmd.FromAccount == "" || cmd.ToAccount == "" {
			return LedgerCommand{}, errForeignCommand
		}
	}
	return cmd, nil
}

func sameLedgerCommand(a, b LedgerCommand) bool {
	return a.Type == b.Type &&
		a.AccountID == b.AccountID &&
		a.InitialCents == b.InitialCents &&
		a.FromAccount == b.FromAccount &&
		a.ToAccount == b.ToAccount &&
		a.Cents == b.Cents &&
		a.IdempotencyKey == b.IdempotencyKey &&
		a.FencingToken == b.FencingToken &&
		a.ProposalID == b.ProposalID
}

func (s *LedgerService) proposeRaft(ctx context.Context, payload []byte) (raft.Ready, error) {
	if waiter, ok := s.raft.(waitingRaftRuntime); ok {
		return waiter.ProposeAndWait(ctx, payload)
	}
	return s.raft.Propose(payload)
}

func (s *LedgerService) usesAsyncApply() bool {
	runtime, ok := s.raft.(applyingRaftRuntime)
	return ok && runtime.AppliesCommittedEntries()
}

func (s *LedgerService) ensureLeader() error {
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

func (s *LedgerService) nextProposalID() string {
	next := atomic.AddUint64(&s.nextID, 1)
	return fmt.Sprintf("ledger-%d-%d", s.serviceID, next)
}

func (s *LedgerService) registerWaiter(proposalID string) <-chan ledgerApplyResult {
	ch := make(chan ledgerApplyResult, 1)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.waiters[proposalID] = ch
	return ch
}

func (s *LedgerService) removeWaiter(proposalID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.waiters, proposalID)
}

func (s *LedgerService) completeWaiterLocked(proposalID string, res ledgerApplyResult) {
	ch, ok := s.waiters[proposalID]
	if !ok {
		return
	}
	delete(s.waiters, proposalID)
	ch <- res
}

func statusFromLedger(err error) error {
	switch {
	case errors.Is(err, statemachine.ErrAccountNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, statemachine.ErrAccountExists),
		errors.Is(err, statemachine.ErrInsufficientFunds),
		errors.Is(err, statemachine.ErrDuplicateTransfer),
		errors.Is(err, statemachine.ErrStaleFencingToken):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, statemachine.ErrInvalidAmount), errors.Is(err, statemachine.ErrNegativeInitialSum):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
