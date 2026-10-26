package transport

import (
	"context"
	"fmt"
	"time"

	"quorum/internal/raft"
	pb "quorum/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Stepper interface {
	Step(raft.Message) (raft.Ready, error)
}

type PeerServer struct {
	pb.UnimplementedRaftPeerServer
	stepper Stepper
}

func NewPeerServer(stepper Stepper) *PeerServer {
	return &PeerServer{stepper: stepper}
}

func (s *PeerServer) Send(_ context.Context, msg *pb.RaftMessage) (*pb.RaftMessages, error) {
	if _, err := s.stepper.Step(FromProtoMessage(msg)); err != nil {
		return nil, err
	}
	return ToProtoMessages(nil), nil
}

type PeerClient struct {
	addrs   map[uint64]string
	timeout time.Duration
}

func NewPeerClient(addrs map[uint64]string, timeout time.Duration) *PeerClient {
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	cp := make(map[uint64]string, len(addrs))
	for id, addr := range addrs {
		cp[id] = addr
	}
	return &PeerClient{addrs: cp, timeout: timeout}
}

func (c *PeerClient) Send(ctx context.Context, msg raft.Message) ([]raft.Message, error) {
	addr, ok := c.addrs[msg.To]
	if !ok {
		return nil, fmt.Errorf("peer %d is not configured", msg.To)
	}
	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	res, err := pb.NewRaftPeerClient(conn).Send(callCtx, ToProtoMessage(msg))
	if err != nil {
		return nil, err
	}
	return FromProtoMessages(res), nil
}
