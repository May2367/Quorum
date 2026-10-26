package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"quorum/client"
	"quorum/internal/linearizability"
	"quorum/internal/raft"
	"quorum/internal/statemachine"
	pb "quorum/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type directRaftSender struct {
	mu    sync.RWMutex
	nodes map[uint64]*AsyncRaft
}

func newDirectRaftSender() *directRaftSender {
	return &directRaftSender{nodes: map[uint64]*AsyncRaft{}}
}

func (s *directRaftSender) Add(id uint64, node *AsyncRaft) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes[id] = node
}

func (s *directRaftSender) Remove(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.nodes, id)
}

func (s *directRaftSender) Send(ctx context.Context, msg raft.Message) ([]raft.Message, error) {
	s.mu.RLock()
	target := s.nodes[msg.To]
	s.mu.RUnlock()
	if target == nil {
		return nil, fmt.Errorf("node %d is not registered", msg.To)
	}
	_, err := target.Step(msg)
	return nil, err
}

type multiNodeKVCluster struct {
	sender         *directRaftSender
	nodes          map[uint64]*AsyncRaft
	services       map[uint64]*KVService
	lockServices   map[uint64]*LockService
	ledgerServices map[uint64]*LedgerService
	machines       map[uint64]*statemachine.KV
	locks          map[uint64]*statemachine.LockTable
	ledgers        map[uint64]*statemachine.Ledger
	servers        map[uint64]*grpc.Server
	listeners      map[uint64]net.Listener
	addrs          map[uint64]string
}

func newMultiNodeKVCluster(t *testing.T) *multiNodeKVCluster {
	t.Helper()
	ids := []uint64{1, 2, 3}
	c := &multiNodeKVCluster{
		sender:         newDirectRaftSender(),
		nodes:          map[uint64]*AsyncRaft{},
		services:       map[uint64]*KVService{},
		lockServices:   map[uint64]*LockService{},
		ledgerServices: map[uint64]*LedgerService{},
		machines:       map[uint64]*statemachine.KV{},
		locks:          map[uint64]*statemachine.LockTable{},
		ledgers:        map[uint64]*statemachine.Ledger{},
		servers:        map[uint64]*grpc.Server{},
		listeners:      map[uint64]net.Listener{},
		addrs:          map[uint64]string{},
	}
	for _, id := range ids {
		machine := statemachine.NewKV()
		locks := statemachine.NewLockTable()
		ledger := statemachine.NewLedger()
		svc := NewKVService(nil, machine)
		lockSvc := NewLockService(nil, locks)
		ledgerSvc := NewLedgerService(nil, ledger)
		electionTimeout := 100
		if id == 1 {
			electionTimeout = 1
		}
		node, err := OpenAsyncRaft(t.TempDir(), id, ids, AsyncRaftOptions{
			Sender:           c.sender,
			SendTimeout:      time.Second,
			ElectionTimeout:  electionTimeout,
			HeartbeatTimeout: 1,
			Apply: func(entries []raft.Entry) {
				svc.ApplyCommitted(entries)
				lockSvc.ApplyCommitted(entries)
				ledgerSvc.ApplyCommitted(entries)
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		svc.SetRaftRuntime(node)
		lockSvc.SetRaftRuntime(node)
		ledgerSvc.SetRaftRuntime(node)
		c.sender.Add(id, node)
		c.nodes[id] = node
		c.services[id] = svc
		c.lockServices[id] = lockSvc
		c.ledgerServices[id] = ledgerSvc
		c.machines[id] = machine
		c.locks[id] = locks
		c.ledgers[id] = ledger
		addr, grpcServer, listener := serveClientServices(t, svc, lockSvc, ledgerSvc)
		c.addrs[id] = addr
		c.servers[id] = grpcServer
		c.listeners[id] = listener
	}
	t.Cleanup(c.Close)
	return c
}

func (c *multiNodeKVCluster) Close() {
	for id := range c.nodes {
		c.StopNode(id)
	}
}

func (c *multiNodeKVCluster) StopNode(id uint64) {
	c.sender.Remove(id)
	if server := c.servers[id]; server != nil {
		server.Stop()
		delete(c.servers, id)
	}
	if listener := c.listeners[id]; listener != nil {
		_ = listener.Close()
		delete(c.listeners, id)
	}
	if node := c.nodes[id]; node != nil {
		_ = node.Close()
		delete(c.nodes, id)
	}
}

func (c *multiNodeKVCluster) ElectLeader(t *testing.T, id uint64) {
	t.Helper()
	c.nodes[id].rt.Node().SetElectionTimeout(1)
	if _, err := c.nodes[id].Tick(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st, err := c.nodes[id].Status()
		if err != nil {
			t.Fatal(err)
		}
		if st.IsLeader() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("node %d did not become leader", id)
}

func (c *multiNodeKVCluster) WaitLeaderKnown(t *testing.T, leader uint64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		known := true
		for id, node := range c.nodes {
			st, err := node.Status()
			if err != nil {
				t.Fatal(err)
			}
			if id == leader {
				if !st.IsLeader() {
					known = false
					break
				}
				continue
			}
			if st.Leader != leader {
				known = false
				break
			}
		}
		if known {
			return
		}
		if _, err := c.nodes[leader].Tick(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("not all nodes learned leader %d", leader)
}

func (c *multiNodeKVCluster) WaitValueOnAll(t *testing.T, key, value string) {
	t.Helper()
	ids := make([]uint64, 0, len(c.machines))
	for id := range c.machines {
		ids = append(ids, id)
	}
	c.WaitValueOnNodes(t, key, value, ids...)
}

func (c *multiNodeKVCluster) WaitValueOnNodes(t *testing.T, key, value string, ids ...uint64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range ids {
			res, err := c.machines[id].Get(key)
			if err != nil || res.Value != value {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, id := range ids {
		res, err := c.machines[id].Get(key)
		if err != nil {
			t.Logf("node %d get %q error: %v", id, key, err)
			continue
		}
		t.Logf("node %d get %q = %+v", id, key, res)
	}
	t.Fatalf("value %q=%q was not applied on nodes %v", key, value, ids)
}

func (c *multiNodeKVCluster) WaitLockOnNodes(t *testing.T, name, holder string, token uint64, ids ...uint64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range ids {
			res, held := c.locks[id].Get(name)
			if !held || res.Holder != holder || res.FencingToken != token {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, id := range ids {
		res, held := c.locks[id].Get(name)
		t.Logf("node %d lock held=%t result=%+v", id, held, res)
	}
	t.Fatalf("lock %q holder=%q token=%d was not applied on nodes %v", name, holder, token, ids)
}

func serveClientServices(t *testing.T, kv *KVService, locks *LockService, ledger *LedgerService) (string, *grpc.Server, net.Listener) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterQuorumServer(server, kv)
	pb.RegisterLockServer(server, NewLockRPCServer(locks))
	pb.RegisterLedgerServer(server, NewLedgerRPCServer(ledger))
	go func() {
		_ = server.Serve(listener)
	}()
	return listener.Addr().String(), server, listener
}

func TestMultiNodeLockAcquireReplicatesFenceToken(t *testing.T) {
	cluster := newMultiNodeKVCluster(t)
	cluster.ElectLeader(t, 1)
	cluster.WaitLeaderKnown(t, 1)

	res, err := cluster.lockServices[1].Acquire(context.Background(), "settlement/job-9", "worker-1", 5)
	if err != nil {
		t.Fatal(err)
	}
	if res.FencingToken == 0 {
		t.Fatalf("fencing token = 0")
	}
	cluster.WaitLockOnNodes(t, "settlement/job-9", "worker-1", res.FencingToken, 1, 2, 3)
}

func TestMultiNodeLedgerRejectsStaleLockHolderTransfer(t *testing.T) {
	cluster := newMultiNodeKVCluster(t)
	cluster.ElectLeader(t, 1)
	cluster.WaitLeaderKnown(t, 1)

	ledger := cluster.ledgerServices[1]
	if err := ledger.CreateAccount(context.Background(), "escrow", 1_000); err != nil {
		t.Fatal(err)
	}
	if err := ledger.CreateAccount(context.Background(), "merchant", 0); err != nil {
		t.Fatal(err)
	}

	first, err := cluster.lockServices[1].Acquire(context.Background(), "settlement/job-stale", "worker-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.lockServices[1].Advance(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	second, err := cluster.lockServices[1].Acquire(context.Background(), "settlement/job-stale", "worker-2", 5)
	if err != nil {
		t.Fatal(err)
	}
	if second.FencingToken <= first.FencingToken {
		t.Fatalf("second token = %d, want > %d", second.FencingToken, first.FencingToken)
	}
	if _, err := ledger.Transfer(context.Background(), "escrow", "merchant", 100, "job-stale-holder", second.FencingToken); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Transfer(context.Background(), "escrow", "merchant", 50, "job-stale-holder", first.FencingToken); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale transfer err = %v, want FailedPrecondition", err)
	}
	balance, err := ledger.GetBalance(context.Background(), "escrow")
	if err != nil {
		t.Fatal(err)
	}
	if balance != 900 {
		t.Fatalf("escrow balance = %d, want 900", balance)
	}
}

func TestMultiNodeSettlementWorkloadSurvivesLeaderFailover(t *testing.T) {
	cluster := newMultiNodeKVCluster(t)
	cluster.ElectLeader(t, 1)
	cluster.WaitLeaderKnown(t, 1)

	c := client.New(client.Config{
		Endpoints:     []string{cluster.addrs[2], cluster.addrs[3], cluster.addrs[1]},
		NodeEndpoints: cluster.addrs,
		Timeout:       time.Second,
	})
	ctx := context.Background()
	const jobs = 12
	const cents = int64(100)
	if err := c.CreateAccount(ctx, "escrow", jobs*cents); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := c.CreateAccount(ctx, fmt.Sprintf("merchant-%d", i), 0); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < jobs/2; i++ {
		jobID := fmt.Sprintf("settlement-%02d", i)
		lock, err := c.AcquireLock(ctx, "settlement/"+jobID, "worker-before", 10)
		if err != nil {
			t.Fatalf("acquire before failover %s: %v", jobID, err)
		}
		if _, err := c.Transfer(ctx, "escrow", fmt.Sprintf("merchant-%d", i%3), cents, jobID, lock.FencingToken); err != nil {
			t.Fatalf("transfer before failover %s: %v", jobID, err)
		}
		if _, err := c.Transfer(ctx, "escrow", fmt.Sprintf("merchant-%d", i%3), cents, jobID, lock.FencingToken); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("duplicate before failover %s err = %v, want FailedPrecondition", jobID, err)
		}
	}

	cluster.StopNode(1)
	cluster.ElectLeader(t, 2)
	cluster.WaitLeaderKnown(t, 2)

	for i := jobs / 2; i < jobs; i++ {
		jobID := fmt.Sprintf("settlement-%02d", i)
		lock, err := c.AcquireLock(ctx, "settlement/"+jobID, "worker-after", 10)
		if err != nil {
			t.Fatalf("acquire after failover %s: %v", jobID, err)
		}
		if _, err := c.Transfer(ctx, "escrow", fmt.Sprintf("merchant-%d", i%3), cents, jobID, lock.FencingToken); err != nil {
			t.Fatalf("transfer after failover %s: %v", jobID, err)
		}
		if _, err := c.Transfer(ctx, "escrow", fmt.Sprintf("merchant-%d", i%3), cents, jobID, lock.FencingToken); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("duplicate after failover %s err = %v, want FailedPrecondition", jobID, err)
		}
	}
	if err := c.CreateAccount(ctx, "fence/escrow", 10); err != nil {
		t.Fatal(err)
	}
	if err := c.CreateAccount(ctx, "fence/merchant", 0); err != nil {
		t.Fatal(err)
	}
	first, err := c.AcquireLock(ctx, "settlement/stale", "stale-worker", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.AdvanceLock(ctx, 1); err != nil {
		t.Fatal(err)
	}
	second, err := c.AcquireLock(ctx, "settlement/stale", "fresh-worker", 5)
	if err != nil {
		t.Fatal(err)
	}
	if second.FencingToken <= first.FencingToken {
		t.Fatalf("second token = %d, want > %d", second.FencingToken, first.FencingToken)
	}
	if _, err := c.Transfer(ctx, "fence/escrow", "fence/merchant", 1, "settlement-stale", second.FencingToken); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Transfer(ctx, "fence/escrow", "fence/merchant", 1, "settlement-stale", first.FencingToken); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale transfer err = %v, want FailedPrecondition", err)
	}

	escrow, err := c.GetBalance(ctx, "escrow")
	if err != nil {
		t.Fatal(err)
	}
	if escrow != 0 {
		t.Fatalf("escrow balance = %d, want 0", escrow)
	}
	fenceEscrow, err := c.GetBalance(ctx, "fence/escrow")
	if err != nil {
		t.Fatal(err)
	}
	fenceMerchant, err := c.GetBalance(ctx, "fence/merchant")
	if err != nil {
		t.Fatal(err)
	}
	var total int64 = escrow + fenceEscrow + fenceMerchant
	for i := 0; i < 3; i++ {
		balance, err := c.GetBalance(ctx, fmt.Sprintf("merchant-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		total += balance
	}
	if total != jobs*cents+10 {
		t.Fatalf("total cents = %d, want %d", total, jobs*cents+10)
	}
	for _, id := range []uint64{2, 3} {
		balance, err := cluster.ledgers[id].GetBalance("escrow")
		if err != nil {
			t.Fatal(err)
		}
		if balance != escrow {
			t.Fatalf("node %d escrow = %d, want %d", id, balance, escrow)
		}
	}
}

func TestMultiNodeKVClientRedirectReplicatesWrite(t *testing.T) {
	cluster := newMultiNodeKVCluster(t)
	cluster.ElectLeader(t, 1)
	cluster.WaitLeaderKnown(t, 1)

	c := client.New(client.Config{
		Endpoints:     []string{cluster.addrs[2], cluster.addrs[3]},
		NodeEndpoints: cluster.addrs,
		Timeout:       time.Second,
	})
	revision, err := c.Put(context.Background(), "jobs/42", "claimed", "client-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if revision != 1 {
		t.Fatalf("revision = %d, want 1", revision)
	}
	cluster.WaitValueOnAll(t, "jobs/42", "claimed")

	reader := client.New(client.Config{
		Endpoints:     []string{cluster.addrs[2]},
		NodeEndpoints: cluster.addrs,
		Timeout:       time.Second,
	})
	value, readRevision, err := reader.Get(context.Background(), "jobs/42")
	if err != nil {
		t.Fatal(err)
	}
	if value != "claimed" || readRevision != 1 {
		t.Fatalf("get value=%q revision=%d, want claimed revision 1", value, readRevision)
	}
}

func TestMultiNodeKVCommittedWriteSurvivesLeaderFailover(t *testing.T) {
	cluster := newMultiNodeKVCluster(t)
	cluster.ElectLeader(t, 1)
	cluster.WaitLeaderKnown(t, 1)

	c := client.New(client.Config{
		Endpoints:     []string{cluster.addrs[2], cluster.addrs[1], cluster.addrs[3]},
		NodeEndpoints: cluster.addrs,
		Timeout:       time.Second,
	})
	if revision, err := c.Put(context.Background(), "failover/key", "before", "failover-c1", 1); err != nil {
		t.Fatal(err)
	} else if revision != 1 {
		t.Fatalf("initial revision = %d, want 1", revision)
	}
	cluster.WaitValueOnAll(t, "failover/key", "before")

	cluster.StopNode(1)
	cluster.ElectLeader(t, 2)
	cluster.WaitLeaderKnown(t, 2)

	value, revision, err := c.Get(context.Background(), "failover/key")
	if err != nil {
		t.Fatal(err)
	}
	if value != "before" || revision != 1 {
		t.Fatalf("after failover get value=%q revision=%d, want before revision 1", value, revision)
	}
	if revision, err := c.Put(context.Background(), "failover/key", "after", "failover-c1", 2); err != nil {
		t.Fatal(err)
	} else if revision != 2 {
		t.Fatalf("post-failover revision = %d, want 2", revision)
	}
	cluster.WaitValueOnNodes(t, "failover/key", "after", 2, 3)
}

func TestMultiNodeKVClientHistoryIsLinearizable(t *testing.T) {
	cluster := newMultiNodeKVCluster(t)
	cluster.ElectLeader(t, 1)
	cluster.WaitLeaderKnown(t, 1)

	seed := client.New(client.Config{Endpoints: []string{cluster.addrs[2], cluster.addrs[3], cluster.addrs[1]}, NodeEndpoints: cluster.addrs, Timeout: time.Second})
	if revision, err := seed.Put(context.Background(), "acct", "0", "mn-seed", 1); err != nil {
		t.Fatal(err)
	} else if revision != 1 {
		t.Fatalf("seed revision = %d, want 1", revision)
	}
	cluster.WaitValueOnAll(t, "acct", "0")

	clients := []*client.Client{
		client.New(client.Config{Endpoints: []string{cluster.addrs[2], cluster.addrs[3], cluster.addrs[1]}, NodeEndpoints: cluster.addrs, Timeout: time.Second}),
		client.New(client.Config{Endpoints: []string{cluster.addrs[3], cluster.addrs[2], cluster.addrs[1]}, NodeEndpoints: cluster.addrs, Timeout: time.Second}),
		client.New(client.Config{Endpoints: []string{cluster.addrs[1], cluster.addrs[2], cluster.addrs[3]}, NodeEndpoints: cluster.addrs, Timeout: time.Second}),
	}
	var clock atomic.Int64
	ops := []func() linearizability.Operation{
		func() linearizability.Operation { return recordClientGet(&clock, clients[0], "acct") },
		func() linearizability.Operation {
			return recordClientCAS(&clock, clients[1], "acct", "0", "10", "mn-c1", 1)
		},
		func() linearizability.Operation { return recordClientGet(&clock, clients[2], "acct") },
		func() linearizability.Operation {
			return recordClientCAS(&clock, clients[0], "acct", "10", "20", "mn-c2", 1)
		},
	}
	var wg sync.WaitGroup
	history := []linearizability.Operation{{ID: 1, Start: 0, End: 0, Kind: linearizability.OpPut, Key: "acct", Value: "0", OK: true, Revision: 1}}
	recorded := make([]linearizability.Operation, len(ops))
	for i, op := range ops {
		i, op := i, op
		wg.Add(1)
		go func() {
			defer wg.Done()
			entry := op()
			entry.ID = i + 2
			recorded[i] = entry
		}()
	}
	wg.Wait()
	history = append(history, recorded...)
	if err := linearizability.CheckKV(history); err != nil {
		t.Fatalf("history is not linearizable: %v\nhistory=%+v", err, history)
	}
}

func recordClientPut(clock *atomic.Int64, c *client.Client, key, value, clientID string, sequence uint64) linearizability.Operation {
	start := clock.Add(1)
	revision, err := c.Put(context.Background(), key, value, clientID, sequence)
	end := clock.Add(1)
	op := linearizability.Operation{Start: start, End: end, Kind: linearizability.OpPut, Key: key, Value: value}
	if err != nil {
		op.Err = clientCodeName(err)
		return op
	}
	op.OK = true
	op.Revision = revision
	return op
}

func recordClientGet(clock *atomic.Int64, c *client.Client, key string) linearizability.Operation {
	start := clock.Add(1)
	value, revision, err := c.Get(context.Background(), key)
	end := clock.Add(1)
	op := linearizability.Operation{Start: start, End: end, Kind: linearizability.OpGet, Key: key}
	if err != nil {
		op.Err = clientCodeName(err)
		return op
	}
	op.OK = true
	op.Current = value
	op.Revision = revision
	return op
}

func recordClientCAS(clock *atomic.Int64, c *client.Client, key, expect, value, clientID string, sequence uint64) linearizability.Operation {
	start := clock.Add(1)
	res, err := c.CompareAndSwap(context.Background(), key, expect, value, clientID, sequence)
	end := clock.Add(1)
	op := linearizability.Operation{Start: start, End: end, Kind: linearizability.OpCAS, Key: key, Expect: expect, Value: value}
	if err != nil {
		op.Err = clientCodeName(err)
		return op
	}
	op.OK = true
	op.Swapped = res.Swapped
	op.Current = res.Current
	op.Revision = res.Revision
	return op
}

func recordClientDelete(clock *atomic.Int64, c *client.Client, key, clientID string, sequence uint64) linearizability.Operation {
	start := clock.Add(1)
	revision, err := c.Delete(context.Background(), key, clientID, sequence)
	end := clock.Add(1)
	op := linearizability.Operation{Start: start, End: end, Kind: linearizability.OpDelete, Key: key}
	if err != nil {
		op.Err = clientCodeName(err)
		return op
	}
	op.OK = true
	op.Revision = revision
	return op
}

func clientCodeName(err error) string {
	if status.Code(err) == codes.NotFound {
		return "NotFound"
	}
	return status.Code(err).String()
}
