package server

import (
	"context"

	pb "quorum/proto"
)

type LedgerRPCServer struct {
	pb.UnimplementedLedgerServer
	ledger *LedgerService
}

func NewLedgerRPCServer(ledger *LedgerService) *LedgerRPCServer {
	return &LedgerRPCServer{ledger: ledger}
}

func (s *LedgerRPCServer) CreateAccount(ctx context.Context, req *pb.CreateAccountRequest) (*pb.CreateAccountResponse, error) {
	if err := s.ledger.CreateAccount(ctx, req.GetAccountId(), req.GetInitialCents()); err != nil {
		return nil, err
	}
	return &pb.CreateAccountResponse{}, nil
}

func (s *LedgerRPCServer) Transfer(ctx context.Context, req *pb.TransferRequest) (*pb.TransferResponse, error) {
	res, err := s.ledger.Transfer(ctx, req.GetFromAccount(), req.GetToAccount(), req.GetCents(), req.GetIdempotencyKey(), req.GetFencingToken())
	if err != nil {
		return nil, err
	}
	return &pb.TransferResponse{
		FromBalance: res.FromBalance,
		ToBalance:   res.ToBalance,
		Revision:    res.Revision,
		Applied:     res.Applied,
	}, nil
}

func (s *LedgerRPCServer) GetBalance(ctx context.Context, req *pb.GetBalanceRequest) (*pb.GetBalanceResponse, error) {
	balance, err := s.ledger.GetBalance(ctx, req.GetAccountId())
	if err != nil {
		return nil, err
	}
	return &pb.GetBalanceResponse{Balance: balance}, nil
}
