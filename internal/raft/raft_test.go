package raft

import "testing"

func TestElectionChoosesLeader(t *testing.T) {
	nodes := newTestCluster()
	nodes[1].SetElectionTimeout(1)
	nodes[1].Tick()
	drain(nodes)

	leaders := leaders(nodes)
	if len(leaders) != 1 {
		t.Fatalf("expected one leader, got %d", len(leaders))
	}
	if leaders[0] != 1 {
		t.Fatalf("expected node 1 leader, got %d", leaders[0])
	}
}

func TestLeaderReplicatesProposalToQuorum(t *testing.T) {
	nodes := newTestCluster()
	nodes[1].SetElectionTimeout(1)
	nodes[1].Tick()
	drain(nodes)

	nodes[1].Step(Message{Type: MsgPropose, Entries: []Entry{{Data: []byte("set x 1")}}})
	drain(nodes)

	for id, n := range nodes {
		if n.Commit() < 2 {
			t.Fatalf("node %d commit index = %d, want at least 2", id, n.Commit())
		}
	}
}

func TestStaleLeaderStepsDownOnHigherTermAppend(t *testing.T) {
	nodes := newTestCluster()
	nodes[1].SetElectionTimeout(1)
	nodes[1].Tick()
	drain(nodes)

	nodes[1].Step(Message{Type: MsgAppend, From: 2, To: 1, Term: nodes[1].Term() + 1})
	if nodes[1].State() != Follower {
		t.Fatalf("node 1 state = %s, want follower", nodes[1].State())
	}
}

func newTestCluster() map[uint64]*Node {
	peers := []uint64{1, 2, 3}
	nodes := map[uint64]*Node{}
	for _, id := range peers {
		nodes[id] = NewNode(id, peers, HardState{}, nil)
	}
	return nodes
}

func drain(nodes map[uint64]*Node) {
	for {
		progress := false
		for _, n := range nodes {
			rd := n.Ready()
			for _, m := range rd.Messages {
				progress = true
				nodes[m.To].Step(m)
			}
		}
		if !progress {
			return
		}
	}
}

func leaders(nodes map[uint64]*Node) []uint64 {
	var out []uint64
	for id, n := range nodes {
		if n.State() == Leader {
			out = append(out, id)
		}
	}
	return out
}
