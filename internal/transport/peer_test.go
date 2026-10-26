package transport

import (
	"context"
	"net"
	"testing"

	"quorum/internal/raft"
	pb "quorum/proto"

	"google.golang.org/grpc"
)

type recordingStepper struct {
	got raft.Message
}

func (s *recordingStepper) Step(m raft.Message) (raft.Ready, error) {
	s.got = m
	return raft.Ready{Messages: []raft.Message{{Type: raft.MsgAppendResponse, From: m.To, To: m.From, Term: m.Term, Index: m.Index}}}, nil
}

func TestPeerClientServerSend(t *testing.T) {
	stepper := &recordingStepper{}
	grpcServer := grpc.NewServer()
	pb.RegisterRaftPeerServer(grpcServer, NewPeerServer(stepper))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	defer grpcServer.Stop()

	client := NewPeerClient(map[uint64]string{2: listener.Addr().String()}, 0)
	responses, err := client.Send(context.Background(), raft.Message{Type: raft.MsgAppend, From: 1, To: 2, Term: 5, Index: 6})
	if err != nil {
		t.Fatal(err)
	}
	if stepper.got.Type != raft.MsgAppend || stepper.got.From != 1 || stepper.got.To != 2 || stepper.got.Term != 5 || stepper.got.Index != 6 {
		t.Fatalf("stepper got %+v", stepper.got)
	}
	if len(responses) != 0 {
		t.Fatalf("responses = %+v, want none for one-way peer delivery", responses)
	}
}

func TestPeerClientMissingPeer(t *testing.T) {
	client := NewPeerClient(nil, 0)
	_, err := client.Send(context.Background(), raft.Message{To: 99})
	if err == nil {
		t.Fatal("expected missing peer error")
	}
}
