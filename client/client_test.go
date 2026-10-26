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

func TestClientDoesNotRetrySemanticErrors(t *testing.T) {
	var calls atomic.Int64
	firstAddr, closeFirst := serveTestQuorum(t, &testQuorumServer{put: func(context.Context, *pb.PutRequest) (*pb.PutResponse, error) {
		calls.Add(1)
		return nil, status.Error(codes.FailedPrecondition, "duplicate transfer")
	}})
	defer closeFirst()
	secondAddr, closeSecond := serveTestQuorum(t, &testQuorumServer{put: func(context.Context, *pb.PutRequest) (*pb.PutResponse, error) {
		calls.Add(1)
		return &pb.PutResponse{Revision: 99}, nil
	}})
	defer closeSecond()

	c := New(Config{Endpoints: []string{firstAddr, secondAddr}, Timeout: time.Second})
	_, err := c.Put(context.Background(), "k", "v", "c1", 1)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("put err = %v, want FailedPrecondition", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
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

type testLockServer struct {
	pb.UnimplementedLockServer
	acquire func(context.Context, *pb.AcquireLockRequest) (*pb.LockResponse, error)
}

func (s *testLockServer) Acquire(ctx context.Context, req *pb.AcquireLockRequest) (*pb.LockResponse, error) {
	return s.acquire(ctx, req)
}

type testLedgerServer struct {
	pb.UnimplementedLedgerServer
	transfer func(context.Context, *pb.TransferRequest) (*pb.TransferResponse, error)
}

func (s *testLedgerServer) Transfer(ctx context.Context, req *pb.TransferRequest) (*pb.TransferResponse, error) {
	return s.transfer(ctx, req)
}

func TestClientLockAndLedgerFollowLeaderRedirectHint(t *testing.T) {
	followerLockAddr, closeFollowerLock := serveTestLock(t, &testLockServer{acquire: func(context.Context, *pb.AcquireLockRequest) (*pb.LockResponse, error) {
		return nil, status.Error(codes.Unavailable, "not leader: leader_id=2")
	}})
	defer closeFollowerLock()
	leaderLockAddr, closeLeaderLock := serveTestLock(t, &testLockServer{acquire: func(context.Context, *pb.AcquireLockRequest) (*pb.LockResponse, error) {
		return &pb.LockResponse{Name: "job", Holder: "worker", FencingToken: 7, ExpiresAt: 5, Acquired: true}, nil
	}})
	defer closeLeaderLock()

	c := New(Config{Endpoints: []string{followerLockAddr}, NodeEndpoints: map[uint64]string{2: leaderLockAddr}, Timeout: time.Second})
	lock, err := c.AcquireLock(context.Background(), "job", "worker", 5)
	if err != nil {
		t.Fatal(err)
	}
	if lock.FencingToken != 7 || !lock.Acquired {
		t.Fatalf("lock = %+v, want token 7 acquired", lock)
	}

	followerLedgerAddr, closeFollowerLedger := serveTestLedger(t, &testLedgerServer{transfer: func(context.Context, *pb.TransferRequest) (*pb.TransferResponse, error) {
		return nil, status.Error(codes.Unavailable, "not leader: leader_id=3")
	}})
	defer closeFollowerLedger()
	leaderLedgerAddr, closeLeaderLedger := serveTestLedger(t, &testLedgerServer{transfer: func(context.Context, *pb.TransferRequest) (*pb.TransferResponse, error) {
		return &pb.TransferResponse{FromBalance: 75, ToBalance: 25, Revision: 4, Applied: true}, nil
	}})
	defer closeLeaderLedger()

	c = New(Config{Endpoints: []string{followerLedgerAddr}, NodeEndpoints: map[uint64]string{3: leaderLedgerAddr}, Timeout: time.Second})
	transfer, err := c.Transfer(context.Background(), "escrow", "merchant", 25, "job-1", 7)
	if err != nil {
		t.Fatal(err)
	}
	if transfer.FromBalance != 75 || transfer.ToBalance != 25 || !transfer.Applied {
		t.Fatalf("transfer = %+v, want applied 75/25", transfer)
	}
}

func serveTestLock(t *testing.T, svc pb.LockServer) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterLockServer(server, svc)
	go func() { _ = server.Serve(listener) }()
	return listener.Addr().String(), func() {
		server.Stop()
		_ = listener.Close()
	}
}

func serveTestLedger(t *testing.T, svc pb.LedgerServer) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterLedgerServer(server, svc)
	go func() { _ = server.Serve(listener) }()
	return listener.Addr().String(), func() {
		server.Stop()
		_ = listener.Close()
	}
}
