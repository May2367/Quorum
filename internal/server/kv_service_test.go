package server

import (
	"context"
	"testing"

	"quorum/internal/statemachine"
	pb "quorum/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestKVServicePutGetAndCAS(t *testing.T) {
	svc, closeFn := newTestKVService(t)
	defer closeFn()

	put, err := svc.Put(context.Background(), &pb.PutRequest{
		Key:      "seat/1",
		Value:    "free",
		ClientId: "c1",
		Sequence: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if put.GetRevision() != 1 {
		t.Fatalf("put revision = %d, want 1", put.GetRevision())
	}

	got, err := svc.Get(context.Background(), &pb.GetRequest{Key: "seat/1"})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetValue() != "free" {
		t.Fatalf("value = %q, want free", got.GetValue())
	}

	cas, err := svc.CompareAndSwap(context.Background(), &pb.CompareAndSwapRequest{
		Key:      "seat/1",
		Expected: "free",
		Value:    "held",
		ClientId: "c2",
		Sequence: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !cas.GetSwapped() || cas.GetRevision() != 2 {
		t.Fatalf("cas response = %+v", cas)
	}

	cas, err = svc.CompareAndSwap(context.Background(), &pb.CompareAndSwapRequest{
		Key:      "seat/1",
		Expected: "free",
		Value:    "sold",
		ClientId: "c3",
		Sequence: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if cas.GetSwapped() {
		t.Fatalf("stale cas swapped unexpectedly: %+v", cas)
	}
	if cas.GetCurrent() != "held" {
		t.Fatalf("current = %q, want held", cas.GetCurrent())
	}
}

func TestKVServiceDelete(t *testing.T) {
	svc, closeFn := newTestKVService(t)
	defer closeFn()

	if _, err := svc.Put(context.Background(), &pb.PutRequest{Key: "jobs/1", Value: "queued", ClientId: "c1", Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Delete(context.Background(), &pb.DeleteRequest{Key: "jobs/1", ClientId: "c1", Sequence: 2}); err != nil {
		t.Fatal(err)
	}
	_, err := svc.Get(context.Background(), &pb.GetRequest{Key: "jobs/1"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("get after delete err = %v, want NotFound", err)
	}
}

func TestKVServiceWriteRequiresCommittedProposal(t *testing.T) {
	rt, err := OpenPersistentRaft(t.TempDir(), 1, []uint64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	svc := NewKVService(rt, statemachine.NewKV())

	_, err = svc.Put(context.Background(), &pb.PutRequest{Key: "x", Value: "1", ClientId: "c1", Sequence: 1})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("put err = %v, want Unavailable", err)
	}
}

func newTestKVService(t *testing.T) (*KVService, func()) {
	t.Helper()
	rt, err := OpenPersistentRaft(t.TempDir(), 1, []uint64{1})
	if err != nil {
		t.Fatal(err)
	}
	rt.Node().SetElectionTimeout(1)
	if _, err := rt.Tick(); err != nil {
		t.Fatal(err)
	}
	return NewKVService(rt, statemachine.NewKV()), func() {
		if err := rt.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestKVServiceUsesAsyncProposalWaiter(t *testing.T) {
	rt, err := OpenAsyncRaft(t.TempDir(), 1, []uint64{1}, AsyncRaftOptions{ElectionTimeout: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	if _, err := rt.Tick(); err != nil {
		t.Fatal(err)
	}
	svc := NewKVService(rt, statemachine.NewKV())
	put, err := svc.Put(context.Background(), &pb.PutRequest{Key: "x", Value: "1", ClientId: "c1", Sequence: 1})
	if err != nil {
		t.Fatal(err)
	}
	if put.GetRevision() != 1 {
		t.Fatalf("revision = %d, want 1", put.GetRevision())
	}
	got, err := svc.Get(context.Background(), &pb.GetRequest{Key: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetValue() != "1" {
		t.Fatalf("value = %q, want 1", got.GetValue())
	}
}
