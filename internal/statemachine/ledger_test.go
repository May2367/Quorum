package statemachine

import (
	"errors"
	"testing"
)

func TestLedgerTransferIsAtomicAndConservesMoney(t *testing.T) {
	ledger := NewLedger()
	mustCreateAccount(t, ledger, "escrow", 10_000)
	mustCreateAccount(t, ledger, "merchant", 0)

	res, err := ledger.Transfer("escrow", "merchant", 2_500, "job-1", 10)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Applied || res.FromBalance != 7_500 || res.ToBalance != 2_500 {
		t.Fatalf("unexpected transfer result: %+v", res)
	}

	escrow, err := ledger.GetBalance("escrow")
	if err != nil {
		t.Fatal(err)
	}
	merchant, err := ledger.GetBalance("merchant")
	if err != nil {
		t.Fatal(err)
	}
	if total := escrow + merchant; total != 10_000 {
		t.Fatalf("total cents = %d, want 10000", total)
	}
}

func TestLedgerIdempotencyRejectsDuplicateTransfer(t *testing.T) {
	ledger := NewLedger()
	mustCreateAccount(t, ledger, "escrow", 10_000)
	mustCreateAccount(t, ledger, "merchant", 0)

	first, err := ledger.Transfer("escrow", "merchant", 1_000, "job-1", 10)
	if err != nil {
		t.Fatal(err)
	}
	dup, err := ledger.Transfer("escrow", "merchant", 1_000, "job-1", 10)
	if !errors.Is(err, ErrDuplicateTransfer) {
		t.Fatalf("duplicate error = %v, want ErrDuplicateTransfer", err)
	}
	if dup != first {
		t.Fatalf("duplicate result = %+v, want %+v", dup, first)
	}

	snap := ledger.Snapshot()
	if snap.Transfers != 1 {
		t.Fatalf("recorded transfers = %d, want 1", snap.Transfers)
	}
	if snap.Accounts["escrow"] != 9_000 || snap.Accounts["merchant"] != 1_000 {
		t.Fatalf("accounts after duplicate = %+v", snap.Accounts)
	}
}

func TestLedgerRejectsStaleFencingToken(t *testing.T) {
	ledger := NewLedger()
	mustCreateAccount(t, ledger, "escrow", 10_000)
	mustCreateAccount(t, ledger, "merchant", 0)

	if _, err := ledger.Transfer("escrow", "merchant", 1_000, "job-1", 20); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Transfer("escrow", "merchant", 500, "job-2", 19); !errors.Is(err, ErrStaleFencingToken) {
		t.Fatalf("stale token error = %v, want ErrStaleFencingToken", err)
	}

	snap := ledger.Snapshot()
	if snap.Accounts["escrow"] != 9_000 || snap.Accounts["merchant"] != 1_000 {
		t.Fatalf("stale transfer mutated accounts: %+v", snap.Accounts)
	}
}

func TestLedgerRejectsOverdraftWithoutPartialMutation(t *testing.T) {
	ledger := NewLedger()
	mustCreateAccount(t, ledger, "escrow", 100)
	mustCreateAccount(t, ledger, "merchant", 0)

	if _, err := ledger.Transfer("escrow", "merchant", 101, "job-1", 1); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("overdraft error = %v, want ErrInsufficientFunds", err)
	}
	snap := ledger.Snapshot()
	if snap.Accounts["escrow"] != 100 || snap.Accounts["merchant"] != 0 {
		t.Fatalf("overdraft mutated accounts: %+v", snap.Accounts)
	}
}

func mustCreateAccount(t *testing.T, ledger *Ledger, accountID string, initialCents int64) {
	t.Helper()
	if err := ledger.CreateAccount(accountID, initialCents); err != nil {
		t.Fatal(err)
	}
}

func TestLedgerNoTokenTransferDoesNotLowerFence(t *testing.T) {
	ledger := NewLedger()
	mustCreateAccount(t, ledger, "escrow", 10_000)
	mustCreateAccount(t, ledger, "merchant", 0)

	if _, err := ledger.Transfer("escrow", "merchant", 1_000, "job-1", 20); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Transfer("escrow", "merchant", 1_000, "job-2", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Transfer("escrow", "merchant", 500, "job-3", 19); !errors.Is(err, ErrStaleFencingToken) {
		t.Fatalf("stale token error = %v, want ErrStaleFencingToken", err)
	}
}
