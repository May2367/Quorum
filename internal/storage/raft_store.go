package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"quorum/internal/raft"
)

const (
	raftWALName = "raft.wal"

	recordHardState byte = 1
	recordEntry     byte = 2
)

var ErrCorruptRaftRecord = errors.New("corrupt raft wal record")

type RaftStore struct {
	wal *WAL
}

type RaftState struct {
	HardState raft.HardState
	Entries   []raft.Entry
}

func OpenRaftStore(dir string) (*RaftStore, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	wal, err := OpenWAL(filepath.Join(dir, raftWALName))
	if err != nil {
		return nil, err
	}
	return &RaftStore{wal: wal}, nil
}

func (s *RaftStore) SaveHardState(hs raft.HardState) error {
	var record [25]byte
	record[0] = recordHardState
	binary.LittleEndian.PutUint64(record[1:9], hs.Term)
	binary.LittleEndian.PutUint64(record[9:17], hs.Vote)
	binary.LittleEndian.PutUint64(record[17:25], hs.Commit)
	return s.wal.Append(record[:])
}

func (s *RaftStore) AppendEntries(entries []raft.Entry) error {
	for _, ent := range entries {
		if err := s.wal.Append(encodeEntry(ent)); err != nil {
			return err
		}
	}
	return nil
}

func (s *RaftStore) Close() error {
	return s.wal.Close()
}

func LoadRaftState(dir string) (RaftState, error) {
	records, err := ReadWAL(filepath.Join(dir, raftWALName))
	if err != nil {
		return RaftState{}, err
	}
	var state RaftState
	for _, record := range records {
		if len(record) == 0 {
			return RaftState{}, ErrCorruptRaftRecord
		}
		switch record[0] {
		case recordHardState:
			hs, err := decodeHardState(record)
			if err != nil {
				return RaftState{}, err
			}
			state.HardState = hs
		case recordEntry:
			ent, err := decodeEntry(record)
			if err != nil {
				return RaftState{}, err
			}
			state.Entries = mergeEntry(state.Entries, ent)
		default:
			return RaftState{}, fmt.Errorf("%w: unknown record type %d", ErrCorruptRaftRecord, record[0])
		}
	}
	return state, nil
}

func encodeEntry(ent raft.Entry) []byte {
	record := make([]byte, 25+len(ent.Data))
	record[0] = recordEntry
	binary.LittleEndian.PutUint64(record[1:9], ent.Index)
	binary.LittleEndian.PutUint64(record[9:17], ent.Term)
	binary.LittleEndian.PutUint64(record[17:25], uint64(len(ent.Data)))
	copy(record[25:], ent.Data)
	return record
}

func decodeHardState(record []byte) (raft.HardState, error) {
	if len(record) != 25 || record[0] != recordHardState {
		return raft.HardState{}, ErrCorruptRaftRecord
	}
	return raft.HardState{
		Term:   binary.LittleEndian.Uint64(record[1:9]),
		Vote:   binary.LittleEndian.Uint64(record[9:17]),
		Commit: binary.LittleEndian.Uint64(record[17:25]),
	}, nil
}

func decodeEntry(record []byte) (raft.Entry, error) {
	if len(record) < 25 || record[0] != recordEntry {
		return raft.Entry{}, ErrCorruptRaftRecord
	}
	size := binary.LittleEndian.Uint64(record[17:25])
	if size > uint64(len(record)-25) || len(record) != 25+int(size) {
		return raft.Entry{}, ErrCorruptRaftRecord
	}
	return raft.Entry{
		Index: binary.LittleEndian.Uint64(record[1:9]),
		Term:  binary.LittleEndian.Uint64(record[9:17]),
		Data:  append([]byte(nil), record[25:]...),
	}, nil
}

func mergeEntry(entries []raft.Entry, ent raft.Entry) []raft.Entry {
	for i, existing := range entries {
		if existing.Index == ent.Index {
			return append(append(entries[:i:i], ent), entries[i+1:]...)
		}
		if existing.Index > ent.Index {
			out := append([]raft.Entry(nil), entries[:i]...)
			out = append(out, ent)
			return out
		}
	}
	return append(entries, ent)
}
