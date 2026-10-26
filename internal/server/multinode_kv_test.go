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
	sender   *directRaftSender
	nodes    map[uint64]*AsyncRaft
	services map[uint64]*KVService
	machines map[uint64]*statemachine.KV
	servers  []*grpc.Server
	addrs    map[uint64]string
}

func newMultiNodeKVCluster(t *testing.T) *multiNodeKVCluster {
	t.Helper()
	ids := []uint64{1, 2, 3}
	c := &multiNodeKVCluster{
		sender:   newDirectRaftSender(),
		nodes:    map[uint64]*AsyncRaft{},
		services: map[uint64]*KVService{},
		machines: map[uint64]*statemachine.KV{},
		addrs:    map[uint64]string{},
	}
	for _, id := range ids {
		machine := statemachine.NewKV()
		svc := NewKVService(nil, machine)
		electionTimeout := 100
		if id == 1 {
			electionTimeout = 1
		}
		node, err := OpenAsyncRaft(t.TempDir(), id, ids, AsyncRaftOptions{
			Sender:           c.sender,
			SendTimeout:      time.Second,
			ElectionTimeout:  electionTimeout,
			HeartbeatTimeout: 1,
			Apply:            svc.ApplyCommitted,
		})
		if err != nil {
			t.Fatal(err)
		}
		svc.SetRaftRuntime(node)
		c.sender.Add(id, node)
		c.nodes[id] = node
		c.services[id] = svc
		c.machines[id] = machine
		c.addrs[id] = serveKVService(t, svc, &c.servers)
	}
	t.Cleanup(c.Close)
	return c
}

func (c *multiNodeKVCluster) Close() {
	for _, server := range c.servers {
		server.Stop()
	}
	for _, node := range c.nodes {
		_ = node.Close()
	}
}

func (c *multiNodeKVCluster) ElectLeader(t *testing.T, id uint64) {
	t.Helper()
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
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, machine := range c.machines {
			res, err := machine.Get(key)
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
	for id, machine := range c.machines {
		res, err := machine.Get(key)
		if err != nil {
			t.Logf("node %d get %q error: %v", id, key, err)
			continue
		}
		t.Logf("node %d get %q = %+v", id, key, res)
	}
	t.Fatalf("value %q=%q was not applied on all nodes", key, value)
}

func serveKVService(t *testing.T, svc *KVService, servers *[]*grpc.Server) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterQuorumServer(server, svc)
	*servers = append(*servers, server)
	go func() {
		_ = server.Serve(listener)
	}()
	return listener.Addr().String()
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
