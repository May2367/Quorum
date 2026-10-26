package client

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "quorum/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type Config struct {
	Endpoints     []string
	NodeEndpoints map[uint64]string
	Timeout       time.Duration
}

type Client struct {
	endpoints     []string
	nodeEndpoints map[uint64]string
	timeout       time.Duration

	mu        sync.Mutex
	preferred string
}

type CASResult struct {
	Swapped  bool
	Current  string
	Revision uint64
}

type LockResult struct {
	Name         string
	Holder       string
	FencingToken uint64
	ExpiresAt    uint64
	Acquired     bool
}

type TransferResult struct {
	FromBalance int64
	ToBalance   int64
	Revision    uint64
	Applied     bool
}

func New(cfg Config) *Client {
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	nodes := make(map[uint64]string, len(cfg.NodeEndpoints))
	for id, endpoint := range cfg.NodeEndpoints {
		nodes[id] = endpoint
	}
	return &Client{endpoints: append([]string(nil), cfg.Endpoints...), nodeEndpoints: nodes, timeout: timeout}
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

func (c *Client) AcquireLock(ctx context.Context, name, holder string, ttlTicks uint64) (LockResult, error) {
	var result LockResult
	err := c.withLockClient(ctx, func(ctx context.Context, lc pb.LockClient) error {
		res, err := lc.Acquire(ctx, &pb.AcquireLockRequest{Name: name, Holder: holder, TtlTicks: ttlTicks})
		if err != nil {
			return err
		}
		result = lockResult(res)
		return nil
	})
	return result, err
}

func (c *Client) KeepAliveLock(ctx context.Context, name, holder string, ttlTicks uint64) (LockResult, error) {
	var result LockResult
	err := c.withLockClient(ctx, func(ctx context.Context, lc pb.LockClient) error {
		res, err := lc.KeepAlive(ctx, &pb.KeepAliveLockRequest{Name: name, Holder: holder, TtlTicks: ttlTicks})
		if err != nil {
			return err
		}
		result = lockResult(res)
		return nil
	})
	return result, err
}

func (c *Client) ReleaseLock(ctx context.Context, name, holder string) error {
	return c.withLockClient(ctx, func(ctx context.Context, lc pb.LockClient) error {
		_, err := lc.Release(ctx, &pb.ReleaseLockRequest{Name: name, Holder: holder})
		return err
	})
}

func (c *Client) CreateAccount(ctx context.Context, accountID string, initialCents int64) error {
	return c.withLedgerClient(ctx, func(ctx context.Context, lc pb.LedgerClient) error {
		_, err := lc.CreateAccount(ctx, &pb.CreateAccountRequest{AccountId: accountID, InitialCents: initialCents})
		return err
	})
}

func (c *Client) Transfer(ctx context.Context, from, to string, cents int64, idempotencyKey string, fencingToken uint64) (TransferResult, error) {
	var result TransferResult
	err := c.withLedgerClient(ctx, func(ctx context.Context, lc pb.LedgerClient) error {
		res, err := lc.Transfer(ctx, &pb.TransferRequest{FromAccount: from, ToAccount: to, Cents: cents, IdempotencyKey: idempotencyKey, FencingToken: fencingToken})
		if err != nil {
			return err
		}
		result = TransferResult{FromBalance: res.GetFromBalance(), ToBalance: res.GetToBalance(), Revision: res.GetRevision(), Applied: res.GetApplied()}
		return nil
	})
	return result, err
}

func (c *Client) GetBalance(ctx context.Context, accountID string) (int64, error) {
	var balance int64
	err := c.withLedgerClient(ctx, func(ctx context.Context, lc pb.LedgerClient) error {
		res, err := lc.GetBalance(ctx, &pb.GetBalanceRequest{AccountId: accountID})
		if err != nil {
			return err
		}
		balance = res.GetBalance()
		return nil
	})
	return balance, err
}

func lockResult(res *pb.LockResponse) LockResult {
	return LockResult{
		Name:         res.GetName(),
		Holder:       res.GetHolder(),
		FencingToken: res.GetFencingToken(),
		ExpiresAt:    res.GetExpiresAt(),
		Acquired:     res.GetAcquired(),
	}
}

func (c *Client) withClient(ctx context.Context, fn func(context.Context, pb.QuorumClient) error) error {
	return c.withConn(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		return fn(ctx, pb.NewQuorumClient(conn))
	})
}

func (c *Client) withLockClient(ctx context.Context, fn func(context.Context, pb.LockClient) error) error {
	return c.withConn(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		return fn(ctx, pb.NewLockClient(conn))
	})
}

func (c *Client) withLedgerClient(ctx context.Context, fn func(context.Context, pb.LedgerClient) error) error {
	return c.withConn(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		return fn(ctx, pb.NewLedgerClient(conn))
	})
}

func (c *Client) withConn(ctx context.Context, fn func(context.Context, *grpc.ClientConn) error) error {
	queue := c.orderedEndpoints()
	if len(queue) == 0 {
		return errors.New("no endpoints configured")
	}
	var lastErr error
	tried := map[string]bool{}
	queued := map[string]bool{}
	for _, endpoint := range queue {
		queued[endpoint] = true
	}
	for len(queue) > 0 {
		endpoint := queue[0]
		queue = queue[1:]
		queued[endpoint] = false
		if tried[endpoint] {
			continue
		}
		tried[endpoint] = true
		callCtx, cancel := context.WithTimeout(ctx, c.timeout)
		conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			cancel()
			lastErr = err
			continue
		}
		err = fn(callCtx, conn)
		closeErr := conn.Close()
		cancel()
		if err == nil && closeErr == nil {
			c.rememberEndpoint(endpoint)
			return nil
		}
		if err != nil {
			lastErr = err
			if redirect := c.redirectEndpoint(err); redirect != "" && !tried[redirect] && !queued[redirect] {
				queue = append([]string{redirect}, queue...)
				queued[redirect] = true
			}
		} else {
			lastErr = closeErr
		}
	}
	return lastErr
}

func (c *Client) orderedEndpoints() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.endpoints))
	if c.preferred != "" {
		out = append(out, c.preferred)
	}
	for _, endpoint := range c.endpoints {
		if endpoint != c.preferred {
			out = append(out, endpoint)
		}
	}
	return out
}

func (c *Client) rememberEndpoint(endpoint string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.preferred = endpoint
}

func (c *Client) redirectEndpoint(err error) string {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unavailable {
		return ""
	}
	leaderID, ok := parseLeaderID(st.Message())
	if !ok || leaderID == 0 {
		return ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodeEndpoints[leaderID]
}

func parseLeaderID(message string) (uint64, bool) {
	const marker = "leader_id="
	idx := strings.Index(message, marker)
	if idx < 0 {
		return 0, false
	}
	rest := message[idx+len(marker):]
	end := len(rest)
	for i, r := range rest {
		if r < '0' || r > '9' {
			end = i
			break
		}
	}
	if end == 0 {
		return 0, false
	}
	id, err := strconv.ParseUint(rest[:end], 10, 64)
	return id, err == nil
}
