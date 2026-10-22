package statemachine

import "testing"

func TestKVApplyCASAndDedup(t *testing.T) {
	kv := NewKV()
	if _, err := kv.Apply(Command{Type: CmdPut, Key: "seat/1", Value: "free", ClientID: "c1", Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	res, err := kv.Apply(Command{Type: CmdCAS, Key: "seat/1", Expect: "free", Value: "held", ClientID: "c2", Sequence: 1})
	if err != nil {
		t.Fatal(err)
	}
	if res.Value != "held" {
		t.Fatalf("value = %q, want held", res.Value)
	}
	dup, err := kv.Apply(Command{Type: CmdCAS, Key: "seat/1", Expect: "free", Value: "held", ClientID: "c2", Sequence: 1})
	if err != nil {
		t.Fatal(err)
	}
	if dup.Revision != res.Revision {
		t.Fatalf("duplicate revision = %d, want %d", dup.Revision, res.Revision)
	}
	if _, err := kv.Apply(Command{Type: CmdCAS, Key: "seat/1", Expect: "free", Value: "sold", ClientID: "c3", Sequence: 1}); err != ErrCompareFailed {
		t.Fatalf("CAS error = %v, want ErrCompareFailed", err)
	}
}

func TestKVWatchReceivesRevisionEvents(t *testing.T) {
	kv := NewKV()
	watch := kv.Watch()
	if _, err := kv.Apply(Command{Type: CmdPut, Key: "jobs/1", Value: "queued"}); err != nil {
		t.Fatal(err)
	}
	ev := <-watch
	if ev.Revision != 1 || ev.Key != "jobs/1" || ev.Value != "queued" {
		t.Fatalf("unexpected event: %+v", ev)
	}
}
