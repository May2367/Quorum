package server

import (
	"testing"

	"quorum/internal/raft"
	"quorum/internal/storage"
)

func TestPersistentRaftPersistsVoteBeforeReturningResponse(t *testing.T) {
	dir := t.TempDir()
	rt, err := OpenPersistentRaft(dir, 1, []uint64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()

	rd, err := rt.Step(raft.Message{
		Type:    raft.MsgRequestVote,
		From:    2,
		To:      1,
		Term:    1,
		Index:   0,
		LogTerm: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rd.Messages) != 1 || rd.Messages[0].Reject {
		t.Fatalf("vote response = %+v", rd.Messages)
	}

	state, err := storage.LoadRaftState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if state.HardState.Term != 1 || state.HardState.Vote != 2 {
		t.Fatalf("hard state after response = %+v", state.HardState)
	}
}

func TestPersistentRaftPersistsAppendBeforeReturningResponse(t *testing.T) {
	dir := t.TempDir()
	rt, err := OpenPersistentRaft(dir, 1, []uint64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()

	rd, err := rt.Step(raft.Message{
		Type:    raft.MsgAppend,
		From:    2,
		To:      1,
		Term:    1,
		Index:   0,
		LogTerm: 0,
		Commit:  1,
		Entries: []raft.Entry{{Index: 1, Term: 1, Data: []byte("set x 1")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rd.Messages) != 1 || rd.Messages[0].Reject {
		t.Fatalf("append response = %+v", rd.Messages)
	}
	if len(rd.Committed) != 1 {
		t.Fatalf("committed entries = %d, want 1", len(rd.Committed))
	}

	state, err := storage.LoadRaftState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Entries) != 1 {
		t.Fatalf("persisted entries = %d, want 1", len(state.Entries))
	}
	if string(state.Entries[0].Data) != "set x 1" {
		t.Fatalf("persisted entry = %+v", state.Entries[0])
	}
	if state.HardState.Commit != 1 {
		t.Fatalf("persisted commit = %d, want 1", state.HardState.Commit)
	}
}

func TestPersistentRaftRecoversStoredState(t *testing.T) {
	dir := t.TempDir()
	rt, err := OpenPersistentRaft(dir, 1, []uint64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Step(raft.Message{
		Type:    raft.MsgAppend,
		From:    2,
		To:      1,
		Term:    4,
		Index:   0,
		LogTerm: 0,
		Commit:  1,
		Entries: []raft.Entry{{Index: 1, Term: 4, Data: []byte("bootstrap")}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, err := OpenPersistentRaft(dir, 1, []uint64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()

	if recovered.Node().Term() != 4 {
		t.Fatalf("term = %d, want 4", recovered.Node().Term())
	}
	if recovered.Node().Commit() != 1 {
		t.Fatalf("commit = %d, want 1", recovered.Node().Commit())
	}
	if recovered.Node().LastIndex() != 1 {
		t.Fatalf("last index = %d, want 1", recovered.Node().LastIndex())
	}
}

func TestPersistentRaftFollowerRestartCatchesCommittedEntry(t *testing.T) {
	cluster := newPersistentTestCluster(t, []uint64{1, 2, 3})
	defer cluster.closeAll()

	cluster.nodes[1].Node().SetElectionTimeout(1)
	cluster.tick(1)
	cluster.drain()
	if cluster.nodes[1].Node().State() != raft.Leader {
		t.Fatalf("node 1 state = %s, want leader", cluster.nodes[1].Node().State())
	}

	cluster.closeNode(3)
	cluster.propose(1, []byte("committed while node 3 is down"))
	cluster.drain()
	if cluster.nodes[1].Node().Commit() < 2 || cluster.nodes[2].Node().Commit() < 2 {
		t.Fatalf("commit indexes after quorum proposal: node1=%d node2=%d", cluster.nodes[1].Node().Commit(), cluster.nodes[2].Node().Commit())
	}

	cluster.restartNode(3)
	for i := 0; i < 10; i++ {
		cluster.tick(1)
		cluster.drain()
	}

	restarted := cluster.nodes[3].Node()
	if restarted.Commit() < 2 {
		t.Fatalf("restarted node commit = %d, want at least 2", restarted.Commit())
	}
	entries := restarted.Entries()
	if len(entries) < 2 {
		t.Fatalf("restarted node entries = %d, want at least 2", len(entries))
	}
	if got := string(entries[1].Data); got != "committed while node 3 is down" {
		t.Fatalf("restarted entry data = %q", got)
	}

	cluster.closeNode(3)
	recovered, err := OpenPersistentRaft(cluster.dirs[3], 3, cluster.peers)
	if err != nil {
		t.Fatal(err)
	}
	cluster.nodes[3] = recovered
	if recovered.Node().Commit() < 2 {
		t.Fatalf("durable restart commit = %d, want at least 2", recovered.Node().Commit())
	}
	entries = recovered.Node().Entries()
	if len(entries) < 2 || string(entries[1].Data) != "committed while node 3 is down" {
		t.Fatalf("durable restart entries = %+v", entries)
	}
}

type persistentTestCluster struct {
	t     *testing.T
	peers []uint64
	dirs  map[uint64]string
	nodes map[uint64]*PersistentRaft
	queue []raft.Message
}

func newPersistentTestCluster(t *testing.T, peers []uint64) *persistentTestCluster {
	t.Helper()
	cluster := &persistentTestCluster{
		t:     t,
		peers: append([]uint64(nil), peers...),
		dirs:  map[uint64]string{},
		nodes: map[uint64]*PersistentRaft{},
	}
	for _, id := range peers {
		cluster.dirs[id] = t.TempDir()
		rt, err := OpenPersistentRaft(cluster.dirs[id], id, cluster.peers)
		if err != nil {
			t.Fatal(err)
		}
		cluster.nodes[id] = rt
	}
	return cluster
}

func (c *persistentTestCluster) tick(id uint64) {
	rd, err := c.nodes[id].Tick()
	if err != nil {
		c.t.Fatal(err)
	}
	c.queue = append(c.queue, rd.Messages...)
}

func (c *persistentTestCluster) propose(id uint64, data []byte) {
	rd, err := c.nodes[id].Propose(data)
	if err != nil {
		c.t.Fatal(err)
	}
	c.queue = append(c.queue, rd.Messages...)
}

func (c *persistentTestCluster) drain() {
	for len(c.queue) > 0 {
		msg := c.queue[0]
		c.queue = c.queue[1:]
		rt := c.nodes[msg.To]
		if rt == nil {
			continue
		}
		rd, err := rt.Step(msg)
		if err != nil {
			c.t.Fatal(err)
		}
		c.queue = append(c.queue, rd.Messages...)
	}
}

func (c *persistentTestCluster) closeNode(id uint64) {
	rt := c.nodes[id]
	if rt == nil {
		return
	}
	if err := rt.Close(); err != nil {
		c.t.Fatal(err)
	}
	delete(c.nodes, id)
	out := c.queue[:0]
	for _, msg := range c.queue {
		if msg.From == id || msg.To == id {
			continue
		}
		out = append(out, msg)
	}
	c.queue = out
}

func (c *persistentTestCluster) restartNode(id uint64) {
	if c.nodes[id] != nil {
		return
	}
	rt, err := OpenPersistentRaft(c.dirs[id], id, c.peers)
	if err != nil {
		c.t.Fatal(err)
	}
	c.nodes[id] = rt
}

func (c *persistentTestCluster) closeAll() {
	for id := range c.nodes {
		c.closeNode(id)
	}
}
