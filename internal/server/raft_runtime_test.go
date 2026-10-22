package server

import (
	"testing"

	"quorum/internal/raft"
	"quorum/internal/storage"
)

func TestPersistentRaftPersistsVoteBeforeReturningResponse(t *testing.T) {
	dir := t.TempDir()
	rt, err := OpenPersistentRaft(dir, 1, []uint64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()

	rd, err := rt.Step(raft.Message{
		Type:    raft.MsgRequestVote,
		From:    2,
		To:      1,
		Term:    1,
		Index:   0,
		LogTerm: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rd.Messages) != 1 || rd.Messages[0].Reject {
		t.Fatalf("vote response = %+v", rd.Messages)
	}

	state, err := storage.LoadRaftState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if state.HardState.Term != 1 || state.HardState.Vote != 2 {
		t.Fatalf("hard state after response = %+v", state.HardState)
	}
}

func TestPersistentRaftPersistsAppendBeforeReturningResponse(t *testing.T) {
	dir := t.TempDir()
	rt, err := OpenPersistentRaft(dir, 1, []uint64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()

	rd, err := rt.Step(raft.Message{
		Type:    raft.MsgAppend,
		From:    2,
		To:      1,
		Term:    1,
		Index:   0,
		LogTerm: 0,
		Commit:  1,
		Entries: []raft.Entry{{Index: 1, Term: 1, Data: []byte("set x 1")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rd.Messages) != 1 || rd.Messages[0].Reject {
		t.Fatalf("append response = %+v", rd.Messages)
	}
	if len(rd.Committed) != 1 {
		t.Fatalf("committed entries = %d, want 1", len(rd.Committed))
	}

	state, err := storage.LoadRaftState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Entries) != 1 {
		t.Fatalf("persisted entries = %d, want 1", len(state.Entries))
	}
	if string(state.Entries[0].Data) != "set x 1" {
		t.Fatalf("persisted entry = %+v", state.Entries[0])
	}
	if state.HardState.Commit != 1 {
		t.Fatalf("persisted commit = %d, want 1", state.HardState.Commit)
	}
}

func TestPersistentRaftRecoversStoredState(t *testing.T) {
	dir := t.TempDir()
	rt, err := OpenPersistentRaft(dir, 1, []uint64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Step(raft.Message{
		Type:    raft.MsgAppend,
		From:    2,
		To:      1,
		Term:    4,
		Index:   0,
		LogTerm: 0,
		Commit:  1,
		Entries: []raft.Entry{{Index: 1, Term: 4, Data: []byte("bootstrap")}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, err := OpenPersistentRaft(dir, 1, []uint64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()

	if recovered.Node().Term() != 4 {
		t.Fatalf("term = %d, want 4", recovered.Node().Term())
	}
	if recovered.Node().Commit() != 1 {
		t.Fatalf("commit = %d, want 1", recovered.Node().Commit())
	}
	if recovered.Node().LastIndex() != 1 {
		t.Fatalf("last index = %d, want 1", recovered.Node().LastIndex())
	}
}
