package statemachine

import (
	"errors"
	"testing"
)

func TestLockAcquireReturnsFencingToken(t *testing.T) {
	locks := NewLockTable()
	res, err := locks.Acquire("settlement/job-1", "worker-1", 5, 42)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Acquired || res.FencingToken != 42 || res.Holder != "worker-1" || res.ExpiresAt != 5 {
		t.Fatalf("lock result = %+v", res)
	}
	if _, err := locks.Acquire("settlement/job-1", "worker-2", 5, 43); !errors.Is(err, ErrLockHeld) {
		t.Fatalf("competing acquire err = %v, want ErrLockHeld", err)
	}
}

func TestLockExpiresAndReacquiresWithHigherFence(t *testing.T) {
	locks := NewLockTable()
	first, err := locks.Acquire("settlement/job-1", "worker-1", 2, 10)
	if err != nil {
		t.Fatal(err)
	}
	locks.Advance(2)
	if _, ok := locks.Get("settlement/job-1"); ok {
		t.Fatal("lock still held after expiry")
	}
	second, err := locks.Acquire("settlement/job-1", "worker-2", 5, 20)
	if err != nil {
		t.Fatal(err)
	}
	if second.FencingToken <= first.FencingToken {
		t.Fatalf("second token = %d, want > %d", second.FencingToken, first.FencingToken)
	}
}

func TestLockKeepAliveRequiresOwner(t *testing.T) {
	locks := NewLockTable()
	if _, err := locks.Acquire("settlement/job-1", "worker-1", 2, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := locks.KeepAlive("settlement/job-1", "worker-2", 5); !errors.Is(err, ErrLockNotOwner) {
		t.Fatalf("keepalive err = %v, want ErrLockNotOwner", err)
	}
	res, err := locks.KeepAlive("settlement/job-1", "worker-1", 5)
	if err != nil {
		t.Fatal(err)
	}
	if res.ExpiresAt != 5 || res.FencingToken != 10 {
		t.Fatalf("keepalive result = %+v", res)
	}
}

func TestLedgerRejectsStaleWorkerTokenAfterLockReacquire(t *testing.T) {
	locks := NewLockTable()
	ledger := NewLedger()
	mustCreateAccount(t, ledger, "escrow", 1_000)
	mustCreateAccount(t, ledger, "merchant", 0)

	first, err := locks.Acquire("settlement/job-1", "worker-1", 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	locks.Advance(1)
	second, err := locks.Acquire("settlement/job-1", "worker-2", 5, 20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Transfer("escrow", "merchant", 100, "job-1", second.FencingToken); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Transfer("escrow", "merchant", 50, "job-1", first.FencingToken); !errors.Is(err, ErrStaleFencingToken) {
		t.Fatalf("stale transfer err = %v, want ErrStaleFencingToken", err)
	}
}
