package server

import (
	"testing"

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
	if _, err := svc.propose(statemachine.Command{Type: statemachine.CmdPut, Key: "a", Value: "1", ClientID: "c1", Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.propose(statemachine.Command{Type: statemachine.CmdCAS, Key: "a", Expect: "1", Value: "2", ClientID: "c1", Sequence: 2}); err != nil {
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
	first, err := svc.propose(statemachine.Command{Type: statemachine.CmdPut, Key: "dedup", Value: "once", ClientID: "c1", Sequence: 1})
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
	_, err = svc.propose(statemachine.Command{Type: statemachine.CmdPut, Key: "uncommitted", Value: "x", ClientID: "c1", Sequence: 1})
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
