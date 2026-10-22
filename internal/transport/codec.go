package transport

import (
	"quorum/internal/raft"
	pb "quorum/proto"
)

func ToProtoMessage(m raft.Message) *pb.RaftMessage {
	out := &pb.RaftMessage{
		Type:       uint32(m.Type),
		From:       m.From,
		To:         m.To,
		Term:       m.Term,
		LogTerm:    m.LogTerm,
		Index:      m.Index,
		Commit:     m.Commit,
		Reject:     m.Reject,
		RejectHint: m.RejectHint,
		Context:    append([]byte(nil), m.Context...),
	}
	for _, ent := range m.Entries {
		out.Entries = append(out.Entries, &pb.RaftEntry{
			Index: ent.Index,
			Term:  ent.Term,
			Data:  append([]byte(nil), ent.Data...),
		})
	}
	return out
}

func FromProtoMessage(m *pb.RaftMessage) raft.Message {
	if m == nil {
		return raft.Message{}
	}
	out := raft.Message{
		Type:       raft.MessageType(m.GetType()),
		From:       m.GetFrom(),
		To:         m.GetTo(),
		Term:       m.GetTerm(),
		LogTerm:    m.GetLogTerm(),
		Index:      m.GetIndex(),
		Commit:     m.GetCommit(),
		Reject:     m.GetReject(),
		RejectHint: m.GetRejectHint(),
		Context:    append([]byte(nil), m.GetContext()...),
	}
	for _, ent := range m.GetEntries() {
		out.Entries = append(out.Entries, raft.Entry{
			Index: ent.GetIndex(),
			Term:  ent.GetTerm(),
			Data:  append([]byte(nil), ent.GetData()...),
		})
	}
	return out
}

func ToProtoMessages(messages []raft.Message) *pb.RaftMessages {
	out := &pb.RaftMessages{}
	for _, m := range messages {
		out.Messages = append(out.Messages, ToProtoMessage(m))
	}
	return out
}

func FromProtoMessages(messages *pb.RaftMessages) []raft.Message {
	if messages == nil {
		return nil
	}
	out := make([]raft.Message, 0, len(messages.GetMessages()))
	for _, m := range messages.GetMessages() {
		out = append(out, FromProtoMessage(m))
	}
	return out
}
