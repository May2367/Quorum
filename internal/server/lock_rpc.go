package server

import (
	"context"

	"quorum/internal/statemachine"
	pb "quorum/proto"
)

type LockRPCServer struct {
	pb.UnimplementedLockServer
	locks *LockService
}

func NewLockRPCServer(locks *LockService) *LockRPCServer {
	return &LockRPCServer{locks: locks}
}

func (s *LockRPCServer) Acquire(ctx context.Context, req *pb.AcquireLockRequest) (*pb.LockResponse, error) {
	res, err := s.locks.Acquire(ctx, req.GetName(), req.GetHolder(), req.GetTtlTicks())
	if err != nil {
		return nil, err
	}
	return lockResponse(res), nil
}

func (s *LockRPCServer) KeepAlive(ctx context.Context, req *pb.KeepAliveLockRequest) (*pb.LockResponse, error) {
	res, err := s.locks.KeepAlive(ctx, req.GetName(), req.GetHolder(), req.GetTtlTicks())
	if err != nil {
		return nil, err
	}
	return lockResponse(res), nil
}

func (s *LockRPCServer) Release(ctx context.Context, req *pb.ReleaseLockRequest) (*pb.ReleaseLockResponse, error) {
	if err := s.locks.Release(ctx, req.GetName(), req.GetHolder()); err != nil {
		return nil, err
	}
	return &pb.ReleaseLockResponse{}, nil
}

func (s *LockRPCServer) Advance(ctx context.Context, req *pb.AdvanceLockRequest) (*pb.AdvanceLockResponse, error) {
	if err := s.locks.Advance(ctx, req.GetTicks()); err != nil {
		return nil, err
	}
	return &pb.AdvanceLockResponse{CurrentTick: s.locks.Tick()}, nil
}

func lockResponse(res statemachine.LockResult) *pb.LockResponse {
	return &pb.LockResponse{
		Name:         res.Name,
		Holder:       res.Holder,
		FencingToken: res.FencingToken,
		ExpiresAt:    res.ExpiresAt,
		Acquired:     res.Acquired,
	}
}
