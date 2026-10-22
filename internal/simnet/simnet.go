package simnet

import "quorum/internal/raft"

type Cluster struct {
	Nodes map[uint64]*raft.Node
	Drop  map[[2]uint64]bool
}

func New(ids ...uint64) *Cluster {
	peers := append([]uint64(nil), ids...)
	c := &Cluster{Nodes: map[uint64]*raft.Node{}, Drop: map[[2]uint64]bool{}}
	for _, id := range ids {
		c.Nodes[id] = raft.NewNode(id, peers, raft.HardState{}, nil)
	}
	return c
}

func (c *Cluster) Partition(a, b uint64) {
	c.Drop[[2]uint64{a, b}] = true
	c.Drop[[2]uint64{b, a}] = true
}

func (c *Cluster) Heal(a, b uint64) {
	delete(c.Drop, [2]uint64{a, b})
	delete(c.Drop, [2]uint64{b, a})
}

func (c *Cluster) Tick(id uint64) {
	c.Nodes[id].Tick()
}

func (c *Cluster) Drain() {
	for {
		progress := false
		for _, n := range c.Nodes {
			rd := n.Ready()
			for _, m := range rd.Messages {
				if c.Drop[[2]uint64{m.From, m.To}] {
					continue
				}
				progress = true
				c.Nodes[m.To].Step(m)
			}
		}
		if !progress {
			return
		}
	}
}
