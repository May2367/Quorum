package client

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	pb "quorum/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testQuorumServer struct {
	pb.UnimplementedQuorumServer
	put func(context.Context, *pb.PutRequest) (*pb.PutResponse, error)
}

func (s *testQuorumServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	return s.put(ctx, req)
}

func TestClientFollowsLeaderRedirectHint(t *testing.T) {
	var followerCalls atomic.Int64
	followerAddr, closeFollower := serveTestQuorum(t, &testQuorumServer{put: func(context.Context, *pb.PutRequest) (*pb.PutResponse, error) {
		followerCalls.Add(1)
		return nil, status.Error(codes.Unavailable, "not leader: leader_id=2")
	}})
	defer closeFollower()

	var leaderCalls atomic.Int64
	leaderAddr, closeLeader := serveTestQuorum(t, &testQuorumServer{put: func(context.Context, *pb.PutRequest) (*pb.PutResponse, error) {
		leaderCalls.Add(1)
		return &pb.PutResponse{Revision: uint64(leaderCalls.Load())}, nil
	}})
	defer closeLeader()

	c := New(Config{
		Endpoints:     []string{followerAddr},
		NodeEndpoints: map[uint64]string{2: leaderAddr},
		Timeout:       time.Second,
	})
	revision, err := c.Put(context.Background(), "k", "v", "c1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if revision != 1 {
		t.Fatalf("revision = %d, want 1", revision)
	}
	if followerCalls.Load() != 1 || leaderCalls.Load() != 1 {
		t.Fatalf("calls follower=%d leader=%d, want 1/1", followerCalls.Load(), leaderCalls.Load())
	}

	revision, err = c.Put(context.Background(), "k", "v2", "c1", 2)
	if err != nil {
		t.Fatal(err)
	}
	if revision != 2 {
		t.Fatalf("revision = %d, want 2", revision)
	}
	if followerCalls.Load() != 1 || leaderCalls.Load() != 2 {
		t.Fatalf("calls follower=%d leader=%d, want 1/2", followerCalls.Load(), leaderCalls.Load())
	}
}

func serveTestQuorum(t *testing.T, svc pb.QuorumServer) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterQuorumServer(server, svc)
	go func() {
		_ = server.Serve(listener)
	}()
	return listener.Addr().String(), func() {
		server.Stop()
		_ = listener.Close()
	}
}
