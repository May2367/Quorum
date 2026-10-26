package server

import (
	"context"
	"testing"

	"quorum/internal/raft"
	"quorum/internal/statemachine"
)

func TestReplayCommittedLogRestoresKVState(t *testing.T) {
	dir := t.TempDir()
	rt, err := OpenPersistentRaft(dir, 1, []uint64{1})
	if err != nil {
		t.Fatal(err)
	}
	rt.Node().SetElectionTimeout(1)
	if _, err := rt.Tick(); err != nil {
		t.Fatal(err)
	}
	svc := NewKVService(rt, statemachine.NewKV())
	if _, err := svc.propose(context.Background(), statemachine.Command{Type: statemachine.CmdPut, Key: "a", Value: "1", ClientID: "c1", Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.propose(context.Background(), statemachine.Command{Type: statemachine.CmdCAS, Key: "a", Expect: "1", Value: "2", ClientID: "c1", Sequence: 2}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Close(); err != nil {
		t.Fatal(err)
	}

	replayed := statemachine.NewKV()
	if err := ReplayCommittedLog(dir, replayed); err != nil {
		t.Fatal(err)
	}
	got, err := replayed.Get("a")
	if err != nil {
		t.Fatal(err)
	}
	if got.Value != "2" || got.Revision != 2 {
		t.Fatalf("replayed value = %+v, want value 2 revision 2", got)
	}
}

func TestReplayCommittedLogRebuildsSessionDedup(t *testing.T) {
	dir := t.TempDir()
	rt, err := OpenPersistentRaft(dir, 1, []uint64{1})
	if err != nil {
		t.Fatal(err)
	}
	rt.Node().SetElectionTimeout(1)
	if _, err := rt.Tick(); err != nil {
		t.Fatal(err)
	}
	svc := NewKVService(rt, statemachine.NewKV())
	first, err := svc.propose(context.Background(), statemachine.Command{Type: statemachine.CmdPut, Key: "dedup", Value: "once", ClientID: "c1", Sequence: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Close(); err != nil {
		t.Fatal(err)
	}

	replayed := statemachine.NewKV()
	if err := ReplayCommittedLog(dir, replayed); err != nil {
		t.Fatal(err)
	}
	dup, err := replayed.Apply(statemachine.Command{Type: statemachine.CmdPut, Key: "dedup", Value: "twice", ClientID: "c1", Sequence: 1})
	if err != nil {
		t.Fatal(err)
	}
	if dup.Revision != first.Revision || dup.Value != first.Value {
		t.Fatalf("duplicate result = %+v, want %+v", dup, first)
	}
	got, err := replayed.Get("dedup")
	if err != nil {
		t.Fatal(err)
	}
	if got.Value != "once" {
		t.Fatalf("dedup value = %q, want once", got.Value)
	}
}

func TestReplayCommittedLogIgnoresUncommittedEntries(t *testing.T) {
	dir := t.TempDir()
	rt, err := OpenPersistentRaft(dir, 1, []uint64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	rt.Node().SetElectionTimeout(1)
	if _, err := rt.Tick(); err != nil {
		t.Fatal(err)
	}
	svc := NewKVService(rt, statemachine.NewKV())
	_, err = svc.propose(context.Background(), statemachine.Command{Type: statemachine.CmdPut, Key: "uncommitted", Value: "x", ClientID: "c1", Sequence: 1})
	if err == nil {
		t.Fatal("expected proposal to remain uncommitted in a 3-node runtime with no peers")
	}
	if err := rt.Close(); err != nil {
		t.Fatal(err)
	}

	replayed := statemachine.NewKV()
	if err := ReplayCommittedLog(dir, replayed); err != nil {
		t.Fatal(err)
	}
	if _, err := replayed.Get("uncommitted"); err != statemachine.ErrKeyNotFound {
		t.Fatalf("get uncommitted err = %v, want ErrKeyNotFound", err)
	}
}

func TestReplayCommittedLogToleratesCommittedCompareFailure(t *testing.T) {
	dir := t.TempDir()
	rt, err := OpenPersistentRaft(dir, 1, []uint64{1})
	if err != nil {
		t.Fatal(err)
	}
	rt.Node().SetElectionTimeout(1)
	if _, err := rt.Tick(); err != nil {
		t.Fatal(err)
	}
	svc := NewKVService(rt, statemachine.NewKV())
	if _, err := svc.propose(context.Background(), statemachine.Command{Type: statemachine.CmdPut, Key: "cas", Value: "current", ClientID: "c1", Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.propose(context.Background(), statemachine.Command{Type: statemachine.CmdCAS, Key: "cas", Expect: "stale", Value: "bad", ClientID: "c2", Sequence: 1}); err == nil {
		t.Fatal("expected stale CAS to fail")
	}
	if err := rt.Close(); err != nil {
		t.Fatal(err)
	}

	replayed := statemachine.NewKV()
	if err := ReplayCommittedLog(dir, replayed); err != nil {
		t.Fatal(err)
	}
	got, err := replayed.Get("cas")
	if err != nil {
		t.Fatal(err)
	}
	if got.Value != "current" || got.Revision != 1 {
		t.Fatalf("replayed value = %+v, want current revision 1", got)
	}
}

func TestReplayCommittedEntriesRestoresLockAndLedgerState(t *testing.T) {
	dir := t.TempDir()
	rt, err := OpenPersistentRaft(dir, 1, []uint64{1})
	if err != nil {
		t.Fatal(err)
	}
	rt.Node().SetElectionTimeout(1)
	if _, err := rt.Tick(); err != nil {
		t.Fatal(err)
	}
	lockSvc := NewLockService(rt, statemachine.NewLockTable())
	ledgerSvc := NewLedgerService(rt, statemachine.NewLedger())
	lock, err := lockSvc.Acquire(context.Background(), "settlement/job-1", "worker-1", 5)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledgerSvc.CreateAccount(context.Background(), "escrow", 500); err != nil {
		t.Fatal(err)
	}
	if err := ledgerSvc.CreateAccount(context.Background(), "merchant", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := ledgerSvc.Transfer(context.Background(), "escrow", "merchant", 125, "job-1", lock.FencingToken); err != nil {
		t.Fatal(err)
	}
	if err := rt.Close(); err != nil {
		t.Fatal(err)
	}

	replayedLocks := statemachine.NewLockTable()
	replayedLedger := statemachine.NewLedger()
	replayedLockSvc := NewLockService(nil, replayedLocks)
	replayedLedgerSvc := NewLedgerService(nil, replayedLedger)
	if err := ReplayCommittedEntries(dir, func(ent raft.Entry) error {
		replayedLockSvc.ApplyCommitted([]raft.Entry{ent})
		replayedLedgerSvc.ApplyCommitted([]raft.Entry{ent})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	gotLock, held := replayedLocks.Get("settlement/job-1")
	if !held || gotLock.Holder != "worker-1" || gotLock.FencingToken != lock.FencingToken {
		t.Fatalf("replayed lock = %+v held=%t, want worker token %d", gotLock, held, lock.FencingToken)
	}
	balance, err := replayedLedger.GetBalance("escrow")
	if err != nil {
		t.Fatal(err)
	}
	if balance != 375 {
		t.Fatalf("replayed escrow balance = %d, want 375", balance)
	}
}
