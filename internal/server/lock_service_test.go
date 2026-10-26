package server

import (
	"context"
	"testing"

	"quorum/internal/raft"
	"quorum/internal/statemachine"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLockServiceAcquireUsesCommittedLogIndexAsFence(t *testing.T) {
	rt, err := OpenPersistentRaft(t.TempDir(), 1, []uint64{1})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	rt.Node().SetElectionTimeout(1)
	if _, err := rt.Tick(); err != nil {
		t.Fatal(err)
	}
	svc := NewLockService(rt, statemachine.NewLockTable())
	res, err := svc.Acquire(context.Background(), "settlement/job-1", "worker-1", 5)
	if err != nil {
		t.Fatal(err)
	}
	if res.FencingToken != 2 {
		t.Fatalf("fencing token = %d, want committed log index 2", res.FencingToken)
	}
	if res.Holder != "worker-1" || !res.Acquired {
		t.Fatalf("lock result = %+v", res)
	}
}

func TestLockServiceRejectsFollowerAcquireWithLeaderHint(t *testing.T) {
	rt := &statusRuntime{status: RaftStatus{ID: 1, Leader: 2, State: raft.Follower}}
	svc := NewLockService(rt, statemachine.NewLockTable())
	_, err := svc.Acquire(context.Background(), "settlement/job-1", "worker-1", 5)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("acquire err = %v, want Unavailable", err)
	}
	if got := status.Convert(err).Message(); got != "not leader: leader_id=2" {
		t.Fatalf("status message = %q, want leader hint", got)
	}
	if rt.proposes != 0 {
		t.Fatalf("proposes = %d, want 0", rt.proposes)
	}
}
