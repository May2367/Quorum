package server

import (
	"quorum/internal/raft"
	"quorum/internal/storage"
)

type RaftStatus struct {
	ID     uint64
	Leader uint64
	State  raft.State
}

func (s RaftStatus) IsLeader() bool {
	return s.State == raft.Leader
}

type PersistentRaft struct {
	node  *raft.Node
	store *storage.RaftStore
}

func OpenPersistentRaft(dir string, id uint64, peers []uint64) (*PersistentRaft, error) {
	state, err := storage.LoadRaftState(dir)
	if err != nil {
		return nil, err
	}
	store, err := storage.OpenRaftStore(dir)
	if err != nil {
		return nil, err
	}
	return &PersistentRaft{
		node:  raft.NewNode(id, peers, state.HardState, state.Entries),
		store: store,
	}, nil
}

func (r *PersistentRaft) Tick() (raft.Ready, error) {
	r.node.Tick()
	return r.drainReady()
}

func (r *PersistentRaft) Step(m raft.Message) (raft.Ready, error) {
	r.node.Step(m)
	return r.drainReady()
}

func (r *PersistentRaft) Propose(data []byte) (raft.Ready, error) {
	r.node.Step(raft.Message{
		Type:    raft.MsgPropose,
		Entries: []raft.Entry{{Data: append([]byte(nil), data...)}},
	})
	return r.drainReady()
}

func (r *PersistentRaft) DrainReady() (raft.Ready, error) {
	return r.drainReady()
}

func (r *PersistentRaft) Node() *raft.Node {
	return r.node
}

func (r *PersistentRaft) Status() (RaftStatus, error) {
	return RaftStatus{ID: r.node.ID(), Leader: r.node.Leader(), State: r.node.State()}, nil
}

func (r *PersistentRaft) Close() error {
	return r.store.Close()
}

func (r *PersistentRaft) drainReady() (raft.Ready, error) {
	rd := r.node.Ready()
	if len(rd.Entries) > 0 {
		if err := r.store.AppendEntries(rd.Entries); err != nil {
			return raft.Ready{}, err
		}
	}
	if err := r.store.SaveHardState(rd.HardState); err != nil {
		return raft.Ready{}, err
	}
	return rd, nil
}
