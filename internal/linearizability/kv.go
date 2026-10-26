package linearizability

import "fmt"

type OperationKind string

const (
	OpGet    OperationKind = "get"
	OpPut    OperationKind = "put"
	OpDelete OperationKind = "delete"
	OpCAS    OperationKind = "cas"
)

type Operation struct {
	ID     int
	Start  int64
	End    int64
	Kind   OperationKind
	Key    string
	Value  string
	Expect string

	OK       bool
	Err      string
	Revision uint64
	Current  string
	Swapped  bool
}

type KVChecker struct {
	values   map[string]modelValue
	revision uint64
}

type modelValue struct {
	value    string
	revision uint64
}

func CheckKV(ops []Operation) error {
	remaining := append([]Operation(nil), ops...)
	placed := map[int]bool{}
	if checkKV(KVChecker{values: map[string]modelValue{}}, remaining, placed) {
		return nil
	}
	return fmt.Errorf("history is not linearizable")
}

func checkKV(model KVChecker, remaining []Operation, placed map[int]bool) bool {
	if len(remaining) == 0 {
		return true
	}
	for i, op := range remaining {
		if !predecessorsPlaced(op, remaining, placed) {
			continue
		}
		next, ok := model.apply(op)
		if !ok {
			continue
		}
		placed[op.ID] = true
		nextRemaining := append([]Operation(nil), remaining[:i]...)
		nextRemaining = append(nextRemaining, remaining[i+1:]...)
		if checkKV(next, nextRemaining, placed) {
			return true
		}
		delete(placed, op.ID)
	}
	return false
}

func predecessorsPlaced(op Operation, remaining []Operation, placed map[int]bool) bool {
	for _, other := range remaining {
		if other.ID == op.ID {
			continue
		}
		if other.End <= op.Start && !placed[other.ID] {
			return false
		}
	}
	return true
}

func (m KVChecker) apply(op Operation) (KVChecker, bool) {
	next := m.clone()
	switch op.Kind {
	case OpPut:
		if !op.OK || op.Err != "" {
			return KVChecker{}, false
		}
		next.revision++
		next.values[op.Key] = modelValue{value: op.Value, revision: next.revision}
		return next, op.Revision == next.revision
	case OpDelete:
		_, ok := next.values[op.Key]
		if !ok {
			return next, !op.OK && op.Err == "NotFound"
		}
		if !op.OK || op.Err != "" {
			return KVChecker{}, false
		}
		next.revision++
		delete(next.values, op.Key)
		return next, op.Revision == next.revision
	case OpGet:
		cur, ok := next.values[op.Key]
		if !ok {
			return next, !op.OK && op.Err == "NotFound"
		}
		return next, op.OK && op.Err == "" && op.Current == cur.value && op.Revision == cur.revision
	case OpCAS:
		cur, ok := next.values[op.Key]
		if !ok {
			return next, !op.OK && op.Err == "NotFound"
		}
		if cur.value != op.Expect {
			return next, op.OK && op.Err == "" && !op.Swapped && op.Current == cur.value && op.Revision == cur.revision
		}
		if !op.OK || op.Err != "" || !op.Swapped {
			return KVChecker{}, false
		}
		next.revision++
		next.values[op.Key] = modelValue{value: op.Value, revision: next.revision}
		return next, op.Current == op.Value && op.Revision == next.revision
	default:
		return KVChecker{}, false
	}
}

func (m KVChecker) clone() KVChecker {
	out := KVChecker{revision: m.revision, values: map[string]modelValue{}}
	for key, value := range m.values {
		out.values[key] = value
	}
	return out
}
