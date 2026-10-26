package statemachine

import (
	"errors"
	"sync"
)

var (
	ErrAccountExists      = errors.New("account already exists")
	ErrAccountNotFound    = errors.New("account not found")
	ErrInvalidAmount      = errors.New("amount must be positive")
	ErrInsufficientFunds  = errors.New("insufficient funds")
	ErrDuplicateTransfer  = errors.New("duplicate transfer")
	ErrStaleFencingToken  = errors.New("stale fencing token")
	ErrNegativeInitialSum = errors.New("initial balance cannot be negative")
)

type Ledger struct {
	mu        sync.Mutex
	accounts  map[string]int64
	transfers map[string]TransferResult
	fences    map[string]uint64
	revision  uint64
}

type TransferResult struct {
	FromBalance int64
	ToBalance   int64
	Revision    uint64
	Applied     bool
}

type LedgerSnapshot struct {
	Revision  uint64
	Accounts  map[string]int64
	Transfers int
}

func NewLedger() *Ledger {
	return &Ledger{
		accounts:  map[string]int64{},
		transfers: map[string]TransferResult{},
		fences:    map[string]uint64{},
	}
}

func (l *Ledger) CreateAccount(accountID string, initialCents int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if initialCents < 0 {
		return ErrNegativeInitialSum
	}
	if _, ok := l.accounts[accountID]; ok {
		return ErrAccountExists
	}
	l.revision++
	l.accounts[accountID] = initialCents
	return nil
}

func (l *Ledger) Transfer(from, to string, cents int64, idempotencyKey string, fencingToken uint64) (TransferResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if cents <= 0 {
		return TransferResult{}, ErrInvalidAmount
	}
	if err := l.checkFenceLocked(idempotencyKey, fencingToken); err != nil {
		return TransferResult{}, err
	}
	if idempotencyKey != "" {
		if prev, ok := l.transfers[idempotencyKey]; ok {
			return prev, ErrDuplicateTransfer
		}
	}

	fromBalance, ok := l.accounts[from]
	if !ok {
		return TransferResult{}, ErrAccountNotFound
	}
	toBalance, ok := l.accounts[to]
	if !ok {
		return TransferResult{}, ErrAccountNotFound
	}
	if fromBalance < cents {
		return TransferResult{}, ErrInsufficientFunds
	}

	l.revision++
	fromBalance -= cents
	toBalance += cents
	l.accounts[from] = fromBalance
	l.accounts[to] = toBalance
	if idempotencyKey != "" && fencingToken > 0 {
		l.fences[idempotencyKey] = fencingToken
	}

	res := TransferResult{
		FromBalance: fromBalance,
		ToBalance:   toBalance,
		Revision:    l.revision,
		Applied:     true,
	}
	if idempotencyKey != "" {
		l.transfers[idempotencyKey] = res
	}
	return res, nil
}

func (l *Ledger) GetBalance(accountID string) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	balance, ok := l.accounts[accountID]
	if !ok {
		return 0, ErrAccountNotFound
	}
	return balance, nil
}

func (l *Ledger) Snapshot() LedgerSnapshot {
	l.mu.Lock()
	defer l.mu.Unlock()

	accounts := make(map[string]int64, len(l.accounts))
	for account, balance := range l.accounts {
		accounts[account] = balance
	}
	return LedgerSnapshot{
		Revision:  l.revision,
		Accounts:  accounts,
		Transfers: len(l.transfers),
	}
}

func (l *Ledger) checkFenceLocked(resourceID string, token uint64) error {
	if resourceID == "" || token == 0 {
		return nil
	}
	if current, ok := l.fences[resourceID]; ok && token < current {
		return ErrStaleFencingToken
	}
	return nil
}
