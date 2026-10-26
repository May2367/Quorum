package server

import (
	"context"
	"testing"

	"quorum/internal/raft"
	"quorum/internal/statemachine"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLedgerServiceReplicatesCreateTransferAndRead(t *testing.T) {
	rt, err := OpenPersistentRaft(t.TempDir(), 1, []uint64{1})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	rt.Node().SetElectionTimeout(1)
	if _, err := rt.Tick(); err != nil {
		t.Fatal(err)
	}
	svc := NewLedgerService(rt, statemachine.NewLedger())
	if err := svc.CreateAccount(context.Background(), "escrow", 500); err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateAccount(context.Background(), "merchant", 0); err != nil {
		t.Fatal(err)
	}
	res, err := svc.Transfer(context.Background(), "escrow", "merchant", 125, "job-1", 10)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Applied || res.FromBalance != 375 || res.ToBalance != 125 {
		t.Fatalf("transfer result = %+v", res)
	}
	balance, err := svc.GetBalance(context.Background(), "escrow")
	if err != nil {
		t.Fatal(err)
	}
	if balance != 375 {
		t.Fatalf("balance = %d, want 375", balance)
	}
}

func TestLedgerServiceRejectsFollowerTransferWithLeaderHint(t *testing.T) {
	rt := &statusRuntime{status: RaftStatus{ID: 1, Leader: 2, State: raft.Follower}}
	svc := NewLedgerService(rt, statemachine.NewLedger())
	_, err := svc.Transfer(context.Background(), "escrow", "merchant", 100, "job-1", 10)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("transfer err = %v, want Unavailable", err)
	}
	if got := status.Convert(err).Message(); got != "not leader: leader_id=2" {
		t.Fatalf("status message = %q, want leader hint", got)
	}
	if rt.proposes != 0 {
		t.Fatalf("proposes = %d, want 0", rt.proposes)
	}
}
