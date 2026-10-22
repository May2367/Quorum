package statemachine

import (
	"errors"
	"sync"
)

var (
	ErrKeyNotFound    = errors.New("key not found")
	ErrCompareFailed  = errors.New("compare failed")
	ErrDuplicateLower = errors.New("sequence number is lower than session watermark")
)

type CommandType uint8

const (
	CmdPut CommandType = iota
	CmdDelete
	CmdCAS
)

type Command struct {
	Type     CommandType
	Key      string
	Value    string
	Expect   string
	ClientID string
	Sequence uint64
	LogIndex uint64
}

type Result struct {
	Value    string
	Revision uint64
	Applied  bool
}

type KV struct {
	mu       sync.Mutex
	values   map[string]versionedValue
	revision uint64
	sessions map[string]sessionResult
	watchers []chan Event
}

type versionedValue struct {
	value    string
	revision uint64
	token    uint64
}

type sessionResult struct {
	sequence uint64
	result   Result
	err      string
}

type Event struct {
	Revision uint64
	Key      string
	Value    string
	Deleted  bool
}

func NewKV() *KV {
	return &KV{
		values:   map[string]versionedValue{},
		sessions: map[string]sessionResult{},
	}
}

func (kv *KV) Apply(cmd Command) (Result, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if cmd.ClientID != "" {
		if prev, ok := kv.sessions[cmd.ClientID]; ok {
			if cmd.Sequence < prev.sequence {
				return Result{}, ErrDuplicateLower
			}
			if cmd.Sequence == prev.sequence {
				return prev.result, decodeErr(prev.err)
			}
		}
	}

	res, err := kv.applyLocked(cmd)
	if cmd.ClientID != "" {
		kv.sessions[cmd.ClientID] = sessionResult{sequence: cmd.Sequence, result: res, err: encodeErr(err)}
	}
	return res, err
}

func (kv *KV) Get(key string) (Result, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	v, ok := kv.values[key]
	if !ok {
		return Result{}, ErrKeyNotFound
	}
	return Result{Value: v.value, Revision: v.revision, Applied: true}, nil
}

func (kv *KV) Watch() <-chan Event {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch := make(chan Event, 128)
	kv.watchers = append(kv.watchers, ch)
	return ch
}

func (kv *KV) Revision() uint64 {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.revision
}

func (kv *KV) applyLocked(cmd Command) (Result, error) {
	switch cmd.Type {
	case CmdPut:
		return kv.putLocked(cmd.Key, cmd.Value, cmd.LogIndex), nil
	case CmdDelete:
		if _, ok := kv.values[cmd.Key]; !ok {
			return Result{}, ErrKeyNotFound
		}
		kv.revision++
		delete(kv.values, cmd.Key)
		kv.publish(Event{Revision: kv.revision, Key: cmd.Key, Deleted: true})
		return Result{Revision: kv.revision, Applied: true}, nil
	case CmdCAS:
		cur, ok := kv.values[cmd.Key]
		if !ok {
			return Result{}, ErrKeyNotFound
		}
		if cur.value != cmd.Expect {
			return Result{Value: cur.value, Revision: cur.revision}, ErrCompareFailed
		}
		return kv.putLocked(cmd.Key, cmd.Value, cmd.LogIndex), nil
	default:
		return Result{}, errors.New("unknown command type")
	}
}

func (kv *KV) putLocked(key, value string, token uint64) Result {
	kv.revision++
	kv.values[key] = versionedValue{value: value, revision: kv.revision, token: token}
	ev := Event{Revision: kv.revision, Key: key, Value: value}
	kv.publish(ev)
	return Result{Value: value, Revision: kv.revision, Applied: true}
}

func (kv *KV) publish(ev Event) {
	for _, ch := range kv.watchers {
		select {
		case ch <- ev:
		default:
		}
	}
}

func encodeErr(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func decodeErr(s string) error {
	switch s {
	case "":
		return nil
	case ErrKeyNotFound.Error():
		return ErrKeyNotFound
	case ErrCompareFailed.Error():
		return ErrCompareFailed
	case ErrDuplicateLower.Error():
		return ErrDuplicateLower
	default:
		return errors.New(s)
	}
}
