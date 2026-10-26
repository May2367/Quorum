package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"quorum/client"
	"quorum/internal/raft"
	"quorum/internal/statemachine"
	pb "quorum/proto"

	"google.golang.org/grpc"
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
}
