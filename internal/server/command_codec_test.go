package server

import (
	"context"
	"errors"
	"testing"

	"quorum/internal/raft"
	"quorum/internal/statemachine"
	pb "quorum/proto"
)

func TestReplicatedServicesIgnoreForeignCommands(t *testing.T) {
	kv := statemachine.NewKV()
	kvSvc := NewKVService(nil, kv)
	locks := statemachine.NewLockTable()
	lockSvc := NewLockService(nil, locks)
	ledger := statemachine.NewLedger()
	ledgerSvc := NewLedgerService(nil, ledger)

	kvData, err := encodeCommand(statemachine.Command{Type: statemachine.CmdPut, Key: "kv/key", Value: "value"})
	if err != nil {
		t.Fatal(err)
	}
	lockData, err := encodeLockCommand(LockCommand{Type: LockCmdAcquire, Name: "settlement/job", Holder: "worker", TTLTicks: 5})
	if err != nil {
		t.Fatal(err)
	}
	ledgerData, err := encodeLedgerCommand(LedgerCommand{Type: LedgerCmdCreateAccount, AccountID: "escrow", InitialCents: 100})
	if err != nil {
		t.Fatal(err)
	}

	kvSvc.ApplyCommitted([]raft.Entry{{Index: 1, Data: lockData}, {Index: 2, Data: ledgerData}})
	if _, err := kv.Get(""); !errors.Is(err, statemachine.ErrKeyNotFound) {
		t.Fatalf("empty kv key err = %v, want ErrKeyNotFound", err)
	}
	if _, err := kv.Get("kv/key"); !errors.Is(err, statemachine.ErrKeyNotFound) {
		t.Fatalf("foreign entries created kv/key err = %v", err)
	}

	lockSvc.ApplyCommitted([]raft.Entry{{Index: 3, Data: kvData}, {Index: 4, Data: ledgerData}})
	if _, held := locks.Get("settlement/job"); held {
		t.Fatal("foreign entries created lock")
	}

	ledgerSvc.ApplyCommitted([]raft.Entry{{Index: 5, Data: kvData}, {Index: 6, Data: lockData}})
	if snap := ledger.Snapshot(); len(snap.Accounts) != 0 {
		t.Fatalf("foreign entries mutated ledger accounts: %+v", snap.Accounts)
	}
}

func TestLedgerServiceIgnoresForeignEntriesBeforeOwnProposal(t *testing.T) {
	rt, err := OpenPersistentRaft(t.TempDir(), 1, []uint64{1})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	rt.Node().SetElectionTimeout(1)
	if _, err := rt.Tick(); err != nil {
		t.Fatal(err)
	}
	ledgerSvc := NewLedgerService(rt, statemachine.NewLedger())
	kvSvc := NewKVService(rt, statemachine.NewKV())

	if _, err := kvSvc.Put(context.Background(), &pb.PutRequest{Key: "kv/key", Value: "value"}); err != nil {
		t.Fatal(err)
	}
	if err := ledgerSvc.CreateAccount(context.Background(), "escrow", 100); err != nil {
		t.Fatal(err)
	}
	balance, err := ledgerSvc.GetBalance(context.Background(), "escrow")
	if err != nil {
		t.Fatal(err)
	}
	if balance != 100 {
		t.Fatalf("balance = %d, want 100", balance)
	}
}
