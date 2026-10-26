package simnet

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"

	"quorum/internal/raft"
)

type Cluster struct {
	Nodes     map[uint64]*raft.Node
	Drop      map[[2]uint64]bool
	Live      map[uint64]bool
	Durable   map[uint64]DurableState
	Committed map[uint64]raft.Entry

	peers []uint64
	queue []raft.Message
	rand  *rand.Rand
}

type DurableState struct {
	HardState raft.HardState
	Entries   []raft.Entry
}

func New(ids ...uint64) *Cluster {
	return NewSeeded(1, ids...)
}

func NewSeeded(seed int64, ids ...uint64) *Cluster {
	peers := append([]uint64(nil), ids...)
	sort.Slice(peers, func(i, j int) bool { return peers[i] < peers[j] })
	c := &Cluster{
		Nodes:     map[uint64]*raft.Node{},
		Drop:      map[[2]uint64]bool{},
		Live:      map[uint64]bool{},
		Durable:   map[uint64]DurableState{},
		Committed: map[uint64]raft.Entry{},
		peers:     peers,
		rand:      rand.New(rand.NewSource(seed)),
	}
	for _, id := range peers {
		c.Nodes[id] = raft.NewNode(id, peers, raft.HardState{}, nil)
		c.Live[id] = true
		c.Durable[id] = DurableState{}
	}
	return c
}

func (c *Cluster) SetElectionTimeout(id uint64, ticks int) {
	if n := c.Nodes[id]; n != nil {
		n.SetElectionTimeout(ticks)
	}
}

func (c *Cluster) Partition(a, b uint64) {
	c.Drop[[2]uint64{a, b}] = true
	c.Drop[[2]uint64{b, a}] = true
}

func (c *Cluster) PartitionGroups(groups ...[]uint64) {
	groupOf := map[uint64]int{}
	for i, group := range groups {
		for _, id := range group {
			groupOf[id] = i
		}
	}
	for _, from := range c.peers {
		for _, to := range c.peers {
			if from == to {
				continue
			}
			if groupOf[from] != groupOf[to] {
				c.Drop[[2]uint64{from, to}] = true
			}
		}
	}
}

func (c *Cluster) Heal(a, b uint64) {
	delete(c.Drop, [2]uint64{a, b})
	delete(c.Drop, [2]uint64{b, a})
}

func (c *Cluster) HealAll() {
	c.Drop = map[[2]uint64]bool{}
}

func (c *Cluster) Tick(id uint64) {
	if !c.Live[id] {
		return
	}
	c.Nodes[id].Tick()
	c.collect(id)
}

func (c *Cluster) TickAll() {
	for _, id := range c.peers {
		c.Tick(id)
	}
}

func (c *Cluster) Propose(id uint64, data []byte) {
	if !c.Live[id] {
		return
	}
	c.Nodes[id].Step(raft.Message{Type: raft.MsgPropose, Entries: []raft.Entry{{Data: append([]byte(nil), data...)}}})
	c.collect(id)
}

func (c *Cluster) Crash(id uint64) {
	c.Live[id] = false
	delete(c.Nodes, id)
	c.dropQueuedFor(id)
}

func (c *Cluster) Restart(id uint64) {
	if c.Live[id] {
		return
	}
	state := c.Durable[id]
	c.Nodes[id] = raft.NewNode(id, c.peers, state.HardState, state.Entries)
	c.Live[id] = true
}

func (c *Cluster) Drain() {
	for c.DeliverRandom() {
	}
}

func (c *Cluster) DeliverRandom() bool {
	if len(c.queue) == 0 {
		return false
	}
	i := c.rand.Intn(len(c.queue))
	msg := c.queue[i]
	c.queue = append(c.queue[:i], c.queue[i+1:]...)
	if c.Drop[[2]uint64{msg.From, msg.To}] || !c.Live[msg.To] || !c.Live[msg.From] {
		return true
	}
	c.Nodes[msg.To].Step(msg)
	c.collect(msg.To)
	return true
}

func (c *Cluster) DropRandomQueued() bool {
	if len(c.queue) == 0 {
		return false
	}
	i := c.rand.Intn(len(c.queue))
	c.queue = append(c.queue[:i], c.queue[i+1:]...)
	return true
}

func (c *Cluster) Leader() (uint64, bool) {
	var leader uint64
	for _, id := range c.peers {
		n := c.Nodes[id]
		if n == nil || n.State() != raft.Leader {
			continue
		}
		if leader != 0 {
			return 0, false
		}
		leader = id
	}
	return leader, leader != 0
}

func (c *Cluster) QueueLen() int {
	return len(c.queue)
}

func (c *Cluster) CheckInvariants() error {
	if err := c.checkOneLeaderPerTerm(); err != nil {
		return err
	}
	if err := c.checkLogMatching(); err != nil {
		return err
	}
	if err := c.checkCommittedEntriesPresent(); err != nil {
		return err
	}
	return nil
}

func (c *Cluster) collect(id uint64) {
	n := c.Nodes[id]
	if n == nil {
		return
	}
	rd := n.Ready()
	state := c.Durable[id]
	if len(rd.Entries) > 0 {
		for _, ent := range rd.Entries {
			state.Entries = upsertEntry(state.Entries, ent)
		}
	}
	state.HardState = rd.HardState
	c.Durable[id] = state
	for _, ent := range rd.Committed {
		if ent.Index == 0 {
			continue
		}
		if prev, ok := c.Committed[ent.Index]; ok && !sameEntry(prev, ent) {
			panic(fmt.Sprintf("conflicting committed entry at index %d", ent.Index))
		}
		c.Committed[ent.Index] = cloneEntry(ent)
	}
	for _, msg := range rd.Messages {
		c.queue = append(c.queue, msg)
	}
}

func (c *Cluster) dropQueuedFor(id uint64) {
	out := c.queue[:0]
	for _, msg := range c.queue {
		if msg.From == id || msg.To == id {
			continue
		}
		out = append(out, msg)
	}
	c.queue = out
}

func (c *Cluster) checkOneLeaderPerTerm() error {
	leaders := map[uint64]uint64{}
	for _, id := range c.peers {
		n := c.Nodes[id]
		if n == nil || n.State() != raft.Leader {
			continue
		}
		if prev, ok := leaders[n.Term()]; ok {
			return fmt.Errorf("term %d has leaders %d and %d", n.Term(), prev, id)
		}
		leaders[n.Term()] = id
	}
	return nil
}

func (c *Cluster) checkLogMatching() error {
	for i, aID := range c.peers {
		a := c.nodeEntries(aID)
		for _, bID := range c.peers[i+1:] {
			b := c.nodeEntries(bID)
			limit := len(a)
			if len(b) < limit {
				limit = len(b)
			}
			for idx := 0; idx < limit; idx++ {
				if a[idx].Term != b[idx].Term {
					continue
				}
				for prefix := 0; prefix <= idx; prefix++ {
					if !sameEntry(a[prefix], b[prefix]) {
						return fmt.Errorf("log matching failed nodes %d/%d index %d prefix %d", aID, bID, idx+1, prefix+1)
					}
				}
			}
		}
	}
	return nil
}

func (c *Cluster) checkCommittedEntriesPresent() error {
	for _, id := range c.peers {
		n := c.Nodes[id]
		if n == nil {
			continue
		}
		entries := n.Entries()
		for index, committed := range c.Committed {
			if n.Commit() < index {
				continue
			}
			if int(index) > len(entries) {
				return fmt.Errorf("node %d commit=%d missing committed index %d", id, n.Commit(), index)
			}
			if !sameEntry(entries[index-1], committed) {
				return fmt.Errorf("node %d committed index %d = %+v, want %+v", id, index, entries[index-1], committed)
			}
		}
	}
	return nil
}

func (c *Cluster) nodeEntries(id uint64) []raft.Entry {
	if n := c.Nodes[id]; n != nil {
		return n.Entries()
	}
	return append([]raft.Entry(nil), c.Durable[id].Entries...)
}

func upsertEntry(entries []raft.Entry, ent raft.Entry) []raft.Entry {
	ent = cloneEntry(ent)
	if ent.Index == 0 {
		return entries
	}
	pos := int(ent.Index - 1)
	if pos < len(entries) {
		entries[pos] = ent
		return entries[:pos+1]
	}
	for len(entries) < pos {
		entries = append(entries, raft.Entry{Index: uint64(len(entries) + 1)})
	}
	return append(entries, ent)
}

func sameEntry(a, b raft.Entry) bool {
	return a.Index == b.Index && a.Term == b.Term && bytes.Equal(a.Data, b.Data)
}

func cloneEntry(ent raft.Entry) raft.Entry {
	ent.Data = append([]byte(nil), ent.Data...)
	return ent
}
