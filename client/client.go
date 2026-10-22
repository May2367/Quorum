package client

import (
	"context"
	"errors"
	"time"

	pb "quorum/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Config struct {
	Endpoints []string
	Timeout   time.Duration
}

type Client struct {
	endpoints []string
	timeout   time.Duration
}

type CASResult struct {
	Swapped  bool
	Current  string
	Revision uint64
}

func New(cfg Config) *Client {
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	return &Client{endpoints: append([]string(nil), cfg.Endpoints...), timeout: timeout}
}

func (c *Client) Endpoints() []string {
	return append([]string(nil), c.endpoints...)
}

func (c *Client) Get(ctx context.Context, key string) (string, uint64, error) {
	var value string
	var revision uint64
	err := c.withClient(ctx, func(ctx context.Context, qc pb.QuorumClient) error {
		res, err := qc.Get(ctx, &pb.GetRequest{Key: key})
		if err != nil {
			return err
		}
		value = res.GetValue()
		revision = res.GetRevision()
		return nil
	})
	return value, revision, err
}

func (c *Client) Put(ctx context.Context, key, value, clientID string, sequence uint64) (uint64, error) {
	var revision uint64
	err := c.withClient(ctx, func(ctx context.Context, qc pb.QuorumClient) error {
		res, err := qc.Put(ctx, &pb.PutRequest{Key: key, Value: value, ClientId: clientID, Sequence: sequence})
		if err != nil {
			return err
		}
		revision = res.GetRevision()
		return nil
	})
	return revision, err
}

func (c *Client) Delete(ctx context.Context, key, clientID string, sequence uint64) (uint64, error) {
	var revision uint64
	err := c.withClient(ctx, func(ctx context.Context, qc pb.QuorumClient) error {
		res, err := qc.Delete(ctx, &pb.DeleteRequest{Key: key, ClientId: clientID, Sequence: sequence})
		if err != nil {
			return err
		}
		revision = res.GetRevision()
		return nil
	})
	return revision, err
}

func (c *Client) CompareAndSwap(ctx context.Context, key, expected, value, clientID string, sequence uint64) (CASResult, error) {
	var result CASResult
	err := c.withClient(ctx, func(ctx context.Context, qc pb.QuorumClient) error {
		res, err := qc.CompareAndSwap(ctx, &pb.CompareAndSwapRequest{Key: key, Expected: expected, Value: value, ClientId: clientID, Sequence: sequence})
		if err != nil {
			return err
		}
		result = CASResult{Swapped: res.GetSwapped(), Current: res.GetCurrent(), Revision: res.GetRevision()}
		return nil
	})
	return result, err
}

func (c *Client) withClient(ctx context.Context, fn func(context.Context, pb.QuorumClient) error) error {
	if len(c.endpoints) == 0 {
		return errors.New("no endpoints configured")
	}
	var lastErr error
	for _, endpoint := range c.endpoints {
		callCtx, cancel := context.WithTimeout(ctx, c.timeout)
		conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			cancel()
			lastErr = err
			continue
		}
		err = fn(callCtx, pb.NewQuorumClient(conn))
		closeErr := conn.Close()
		cancel()
		if err == nil && closeErr == nil {
			return nil
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = closeErr
		}
	}
	return lastErr
}
