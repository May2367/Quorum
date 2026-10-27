package storage

import (
	"path/filepath"
	"testing"

	"quorum/internal/raft"
)

func TestRaftStorePersistsHardStateAndEntries(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenRaftStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveHardState(raft.HardState{Term: 3, Vote: 2, Commit: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1, Data: []byte("bootstrap")},
		{Index: 2, Term: 3, Data: []byte("set x 1")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveHardState(raft.HardState{Term: 3, Vote: 2, Commit: 2}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	state, err := LoadRaftState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if state.HardState != (raft.HardState{Term: 3, Vote: 2, Commit: 2}) {
		t.Fatalf("hard state = %+v", state.HardState)
	}
	if len(state.Entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(state.Entries))
	}
	if got := string(state.Entries[1].Data); got != "set x 1" {
		t.Fatalf("entry data = %q", got)
	}
}

func TestRaftStoreSaveReadyPersistsEntriesAndHardState(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenRaftStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveReady([]raft.Entry{
		{Index: 1, Term: 2, Data: []byte("one")},
		{Index: 2, Term: 2, Data: []byte("two")},
	}, raft.HardState{Term: 2, Vote: 1, Commit: 2}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	state, err := LoadRaftState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if state.HardState != (raft.HardState{Term: 2, Vote: 1, Commit: 2}) {
		t.Fatalf("hard state = %+v", state.HardState)
	}
	if len(state.Entries) != 2 || string(state.Entries[0].Data) != "one" || string(state.Entries[1].Data) != "two" {
		t.Fatalf("entries = %+v", state.Entries)
	}
}

func TestRaftStoreRecoveryCanSeedNode(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenRaftStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveHardState(raft.HardState{Term: 7, Vote: 3, Commit: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendEntries([]raft.Entry{{Index: 1, Term: 7, Data: []byte("noop")}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	state, err := LoadRaftState(dir)
	if err != nil {
		t.Fatal(err)
	}
	node := raft.NewNode(1, []uint64{1, 2, 3}, state.HardState, state.Entries)
	if node.Term() != 7 {
		t.Fatalf("node term = %d, want 7", node.Term())
	}
	if node.Commit() != 1 {
		t.Fatalf("node commit = %d, want 1", node.Commit())
	}
	if node.LastIndex() != 1 {
		t.Fatalf("node last index = %d, want 1", node.LastIndex())
	}
}

func TestRaftStoreLoadMissingWAL(t *testing.T) {
	state, err := LoadRaftState(filepath.Join(t.TempDir(), "node"))
	if err != nil {
		t.Fatal(err)
	}
	if state.HardState != (raft.HardState{}) {
		t.Fatalf("hard state = %+v", state.HardState)
	}
	if len(state.Entries) != 0 {
		t.Fatalf("entries = %d, want 0", len(state.Entries))
	}
}

func TestRaftStoreEntryRewriteKeepsLatestIndexRecord(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenRaftStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AppendEntries([]raft.Entry{{Index: 1, Term: 1, Data: []byte("old")}}); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendEntries([]raft.Entry{{Index: 1, Term: 2, Data: []byte("new")}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	state, err := LoadRaftState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(state.Entries))
	}
	if state.Entries[0].Term != 2 || string(state.Entries[0].Data) != "new" {
		t.Fatalf("entry = %+v", state.Entries[0])
	}
}
