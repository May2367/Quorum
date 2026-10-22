package transport

import (
	"reflect"
	"testing"

	"quorum/internal/raft"
)

func TestMessageRoundTrip(t *testing.T) {
	msg := raft.Message{
		Type:       raft.MsgAppend,
		From:       1,
		To:         2,
		Term:       3,
		LogTerm:    2,
		Index:      9,
		Commit:     8,
		Reject:     true,
		RejectHint: 7,
		Context:    []byte("ctx"),
		Entries: []raft.Entry{
			{Index: 10, Term: 3, Data: []byte("a")},
			{Index: 11, Term: 3, Data: []byte("b")},
		},
	}
	got := FromProtoMessage(ToProtoMessage(msg))
	if !reflect.DeepEqual(got, msg) {
		t.Fatalf("round trip = %+v, want %+v", got, msg)
	}
}

func TestMessagesRoundTrip(t *testing.T) {
	messages := []raft.Message{
		{Type: raft.MsgRequestVote, From: 1, To: 2, Term: 4},
		{Type: raft.MsgRequestVoteResponse, From: 2, To: 1, Term: 4},
	}
	got := FromProtoMessages(ToProtoMessages(messages))
	if !reflect.DeepEqual(got, messages) {
		t.Fatalf("round trip = %+v, want %+v", got, messages)
	}
}
