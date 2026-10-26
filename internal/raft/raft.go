package raft

import "sort"

type Node struct {
	id    uint64
	peers []uint64

	state  State
	term   uint64
	vote   uint64
	leader uint64

	log     []Entry
	commit  uint64
	applied uint64

	votes map[uint64]bool
	match map[uint64]uint64
	next  map[uint64]uint64

	electionElapsed  int
	heartbeatElapsed int
	electionTimeout  int
	heartbeatTimeout int

	msgs      []Message
	unstable  []Entry
	committed []Entry
}

func NewNode(id uint64, peers []uint64, hs HardState, entries []Entry) *Node {
	cp := append([]uint64(nil), peers...)
	log := append([]Entry{{Index: 0, Term: 0}}, entries...)
	n := &Node{
		id:               id,
		peers:            cp,
		state:            Follower,
		term:             hs.Term,
		vote:             hs.Vote,
		commit:           hs.Commit,
		applied:          hs.Commit,
		votes:            map[uint64]bool{},
		match:            map[uint64]uint64{},
		next:             map[uint64]uint64{},
		electionTimeout:  10 + int(id%5),
		heartbeatTimeout: 2,
		log:              log,
	}
	for _, p := range cp {
		n.match[p] = 0
		n.next[p] = n.lastIndex() + 1
	}
	return n
}

func (n *Node) ID() uint64        { return n.id }
func (n *Node) State() State      { return n.state }
func (n *Node) Term() uint64      { return n.term }
func (n *Node) Leader() uint64    { return n.leader }
func (n *Node) Commit() uint64    { return n.commit }
func (n *Node) LastIndex() uint64 { return n.lastIndex() }

func (n *Node) Entries() []Entry {
	out := make([]Entry, 0, len(n.log)-1)
	for _, ent := range n.log[1:] {
		out = append(out, cloneEntry(ent))
	}
	return out
}

func (n *Node) SetElectionTimeout(ticks int) {
	if ticks < 1 {
		ticks = 1
	}
	n.electionTimeout = ticks
}

func (n *Node) SetHeartbeatTimeout(ticks int) {
	if ticks < 1 {
		ticks = 1
	}
	n.heartbeatTimeout = ticks
}

func (n *Node) Tick() {
	switch n.state {
	case Leader:
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.heartbeatTimeout {
			n.heartbeatElapsed = 0
			for _, p := range n.peers {
				if p != n.id {
					n.sendAppend(p)
				}
			}
		}
	default:
		n.electionElapsed++
		if n.electionElapsed >= n.electionTimeout {
			n.electionElapsed = 0
			n.campaign()
		}
	}
}

func (n *Node) Step(m Message) {
	if m.Term > n.term {
		n.becomeFollower(m.Term, 0)
	}
	switch m.Type {
	case MsgRequestVote:
		n.handleRequestVote(m)
	case MsgRequestVoteResponse:
		n.handleVoteResponse(m)
	case MsgAppend, MsgHeartbeat:
		n.handleAppend(m)
	case MsgAppendResponse, MsgHeartbeatResponse:
		n.handleAppendResponse(m)
	case MsgPropose:
		n.handlePropose(m)
	}
}

func (n *Node) Ready() Ready {
	rd := Ready{
		HardState: HardState{Term: n.term, Vote: n.vote, Commit: n.commit},
		Entries:   append([]Entry(nil), n.unstable...),
		Messages:  append([]Message(nil), n.msgs...),
		Committed: append([]Entry(nil), n.committed...),
	}
	n.msgs = nil
	n.unstable = nil
	n.committed = nil
	return rd
}

func (n *Node) campaign() {
	n.becomeCandidate()
	if n.quorum() == 1 {
		n.becomeLeader()
		return
	}
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		n.send(Message{
			Type:    MsgRequestVote,
			To:      p,
			Term:    n.term,
			Index:   n.lastIndex(),
			LogTerm: n.lastTerm(),
		})
	}
}

func (n *Node) becomeFollower(term, leader uint64) {
	n.state = Follower
	n.term = term
	n.leader = leader
	n.vote = 0
	n.votes = map[uint64]bool{}
	n.electionElapsed = 0
}

func (n *Node) becomeCandidate() {
	n.state = Candidate
	n.term++
	n.vote = n.id
	n.leader = 0
	n.votes = map[uint64]bool{n.id: true}
	n.electionElapsed = 0
}

func (n *Node) becomeLeader() {
	n.state = Leader
	n.leader = n.id
	n.heartbeatElapsed = 0
	last := n.lastIndex()
	for _, p := range n.peers {
		n.match[p] = 0
		n.next[p] = last + 1
	}
	n.match[n.id] = last
	n.appendEntries([]Entry{{Term: n.term}})
	n.match[n.id] = n.lastIndex()
	for _, p := range n.peers {
		if p != n.id {
			n.sendAppend(p)
		}
	}
	n.maybeCommit()
}

func (n *Node) handleRequestVote(m Message) {
	reject := true
	if m.Term >= n.term {
		canVote := n.vote == 0 || n.vote == m.From
		upToDate := m.LogTerm > n.lastTerm() || (m.LogTerm == n.lastTerm() && m.Index >= n.lastIndex())
		if canVote && upToDate {
			n.vote = m.From
			n.electionElapsed = 0
			reject = false
		}
	}
	n.send(Message{Type: MsgRequestVoteResponse, To: m.From, Term: n.term, Reject: reject})
}

func (n *Node) handleVoteResponse(m Message) {
	if n.state != Candidate || m.Term != n.term {
		return
	}
	n.votes[m.From] = !m.Reject
	granted, rejected := 0, 0
	for _, ok := range n.votes {
		if ok {
			granted++
		} else {
			rejected++
		}
	}
	if granted >= n.quorum() {
		n.becomeLeader()
		return
	}
	if rejected >= n.quorum() {
		n.becomeFollower(n.term, 0)
	}
}

func (n *Node) handleAppend(m Message) {
	if m.Term < n.term {
		n.send(Message{Type: MsgAppendResponse, To: m.From, Term: n.term, Reject: true, RejectHint: n.lastIndex()})
		return
	}
	n.becomeFollower(m.Term, m.From)
	if !n.matchLog(m.Index, m.LogTerm) {
		n.send(Message{Type: MsgAppendResponse, To: m.From, Term: n.term, Index: m.Index, Reject: true, RejectHint: n.lastIndex()})
		return
	}
	for _, ent := range m.Entries {
		if ent.Index <= n.lastIndex() {
			if n.termAt(ent.Index) != ent.Term {
				n.truncate(ent.Index - 1)
				n.log = append(n.log, cloneEntry(ent))
				n.unstable = append(n.unstable, cloneEntry(ent))
			}
			continue
		}
		n.log = append(n.log, cloneEntry(ent))
		n.unstable = append(n.unstable, cloneEntry(ent))
	}
	if m.Commit > n.commit {
		n.commitTo(min(m.Commit, n.lastIndex()))
	}
	n.send(Message{Type: MsgAppendResponse, To: m.From, Term: n.term, Index: n.lastIndex()})
}

func (n *Node) handleAppendResponse(m Message) {
	if n.state != Leader || m.Term != n.term {
		return
	}
	if m.Reject {
		next := m.RejectHint
		if next > 0 {
			next++
		} else if n.next[m.From] > 1 {
			next = n.next[m.From] - 1
		} else {
			next = 1
		}
		n.next[m.From] = next
		n.sendAppend(m.From)
		return
	}
	n.match[m.From] = m.Index
	n.next[m.From] = m.Index + 1
	if n.maybeCommit() {
		for _, p := range n.peers {
			if p != n.id {
				n.sendAppend(p)
			}
		}
	}
}

func (n *Node) handlePropose(m Message) {
	if n.state != Leader {
		return
	}
	var entries []Entry
	for _, ent := range m.Entries {
		entries = append(entries, Entry{Term: n.term, Data: append([]byte(nil), ent.Data...)})
	}
	n.appendEntries(entries)
	n.match[n.id] = n.lastIndex()
	for _, p := range n.peers {
		if p != n.id {
			n.sendAppend(p)
		}
	}
	n.maybeCommit()
}

func (n *Node) sendAppend(to uint64) {
	next := n.next[to]
	if next == 0 {
		next = n.lastIndex() + 1
	}
	prev := next - 1
	entries := append([]Entry(nil), n.entriesFrom(next)...)
	n.send(Message{
		Type:    MsgAppend,
		To:      to,
		Term:    n.term,
		Index:   prev,
		LogTerm: n.termAt(prev),
		Commit:  n.commit,
		Entries: entries,
	})
}

func (n *Node) maybeCommit() bool {
	matches := make([]uint64, 0, len(n.peers))
	for _, p := range n.peers {
		if p == n.id {
			matches = append(matches, n.lastIndex())
		} else {
			matches = append(matches, n.match[p])
		}
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	candidate := matches[n.quorum()-1]
	if candidate > n.commit && n.termAt(candidate) == n.term {
		n.commitTo(candidate)
		return true
	}
	return false
}

func (n *Node) appendEntries(entries []Entry) {
	for _, ent := range entries {
		ent.Index = n.lastIndex() + 1
		ent.Term = n.term
		n.log = append(n.log, cloneEntry(ent))
		n.unstable = append(n.unstable, cloneEntry(ent))
	}
}

func (n *Node) commitTo(index uint64) {
	if index <= n.commit {
		return
	}
	for i := n.commit + 1; i <= index; i++ {
		n.committed = append(n.committed, cloneEntry(n.log[i]))
	}
	n.commit = index
}

func (n *Node) matchLog(index, term uint64) bool {
	return index <= n.lastIndex() && n.termAt(index) == term
}

func (n *Node) lastIndex() uint64 {
	return n.log[len(n.log)-1].Index
}

func (n *Node) lastTerm() uint64 {
	return n.log[len(n.log)-1].Term
}

func (n *Node) termAt(index uint64) uint64 {
	if index >= uint64(len(n.log)) {
		return 0
	}
	return n.log[index].Term
}

func (n *Node) entriesFrom(index uint64) []Entry {
	if index > n.lastIndex() {
		return nil
	}
	out := make([]Entry, 0, n.lastIndex()-index+1)
	for i := index; i <= n.lastIndex(); i++ {
		out = append(out, cloneEntry(n.log[i]))
	}
	return out
}

func (n *Node) truncate(index uint64) {
	n.log = n.log[:index+1]
}

func (n *Node) send(m Message) {
	m.From = n.id
	if m.Term == 0 {
		m.Term = n.term
	}
	n.msgs = append(n.msgs, m)
}

func (n *Node) quorum() int {
	return len(n.peers)/2 + 1
}

func cloneEntry(ent Entry) Entry {
	ent.Data = append([]byte(nil), ent.Data...)
	return ent
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
