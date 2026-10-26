package server

import (
	"context"
	"testing"
	"time"

	"quorum/internal/raft"
	"quorum/internal/storage"
)

func TestAsyncRaftSerializesAndPersistsProposal(t *testing.T) {
	driver, err := OpenAsyncRaft(t.TempDir(), 1, []uint64{1}, AsyncRaftOptions{ElectionTimeout: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()

	if _, err := driver.Tick(); err != nil {
		t.Fatal(err)
	}
	rd, err := driver.Propose([]byte("settle job-1"))
	if err != nil {
		t.Fatal(err)
	}
	if len(rd.Committed) != 1 {
		t.Fatalf("committed entries = %d, want 1", len(rd.Committed))
	}
	if string(rd.Committed[0].Data) != "settle job-1" {
		t.Fatalf("committed data = %q", string(rd.Committed[0].Data))
	}
}

func TestAsyncRaftPersistsBeforeDispatchingMessages(t *testing.T) {
	dir := t.TempDir()
	sender := &recordingSender{seen: make(chan raft.Message, 1)}
	driver, err := OpenAsyncRaft(dir, 1, []uint64{1, 2}, AsyncRaftOptions{
		ElectionTimeout: 1,
		Sender:          sender,
		SendTimeout:     time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()

	if _, err := driver.Tick(); err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-sender.seen:
		if msg.Type != raft.MsgRequestVote {
			t.Fatalf("message type = %v, want request vote", msg.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for outbound raft message")
	}

	state, err := storage.LoadRaftState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if state.HardState.Term != 1 || state.HardState.Vote != 1 {
		t.Fatalf("hard state = %+v, want term=1 vote=1", state.HardState)
	}
}

func TestAsyncRaftCloseRejectsRequests(t *testing.T) {
	driver, err := OpenAsyncRaft(t.TempDir(), 1, []uint64{1}, AsyncRaftOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := driver.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Tick(); err != ErrAsyncRaftClosed {
		t.Fatalf("tick after close error = %v, want ErrAsyncRaftClosed", err)
	}
}

type recordingSender struct {
	seen chan raft.Message
}

func (s *recordingSender) Send(_ context.Context, msg raft.Message) ([]raft.Message, error) {
	s.seen <- msg
	return nil, nil
}

func TestAsyncRaftProposeAndWaitReturnsCommittedReady(t *testing.T) {
	driver, err := OpenAsyncRaft(t.TempDir(), 1, []uint64{1}, AsyncRaftOptions{ElectionTimeout: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()
	if _, err := driver.Tick(); err != nil {
		t.Fatal(err)
	}
	rd, err := driver.ProposeAndWait(context.Background(), []byte("set x 1"))
	if err != nil {
		t.Fatal(err)
	}
	if len(rd.Committed) != 1 || string(rd.Committed[0].Data) != "set x 1" {
		t.Fatalf("committed = %+v", rd.Committed)
	}
}

func TestAsyncRaftProposeAndWaitHonorsContext(t *testing.T) {
	driver, err := OpenAsyncRaft(t.TempDir(), 1, []uint64{1, 2, 3}, AsyncRaftOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := driver.ProposeAndWait(ctx, []byte("will not commit")); err == nil {
		t.Fatal("expected context timeout")
	}
}
