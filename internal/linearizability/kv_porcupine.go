package linearizability

import (
	"fmt"
	"time"

	"github.com/anishathalye/porcupine"
)

type porcupineInput struct {
	kind   OperationKind
	key    string
	value  string
	expect string
}

type porcupineOutput struct {
	ok       bool
	err      string
	revision uint64
	current  string
	swapped  bool
}

type porcupineState struct {
	values   map[string]modelValue
	revision uint64
}

func CheckKVPorcupine(ops []Operation) error {
	result := porcupine.CheckOperationsTimeout(kvPorcupineModel(), porcupineOperations(ops), 5*time.Second)
	if result != porcupine.Ok {
		return fmt.Errorf("porcupine result: %s", result)
	}
	return nil
}

func porcupineOperations(ops []Operation) []porcupine.Operation {
	out := make([]porcupine.Operation, 0, len(ops))
	for _, op := range ops {
		out = append(out, porcupine.Operation{
			ClientId: op.ID,
			Call:     op.Start,
			Return:   op.End,
			Input: porcupineInput{
				kind:   op.Kind,
				key:    op.Key,
				value:  op.Value,
				expect: op.Expect,
			},
			Output: porcupineOutput{
				ok:       op.OK,
				err:      op.Err,
				revision: op.Revision,
				current:  op.Current,
				swapped:  op.Swapped,
			},
			Metadata: op,
		})
	}
	return out
}

func kvPorcupineModel() porcupine.Model {
	return porcupine.Model{
		Init: func() interface{} {
			return porcupineState{values: map[string]modelValue{}}
		},
		Step: func(state interface{}, input interface{}, output interface{}) (bool, interface{}) {
			st := state.(porcupineState)
			in := input.(porcupineInput)
			out := output.(porcupineOutput)
			next, ok := applyPorcupineOperation(st, in, out)
			return ok, next
		},
		Equal: func(a interface{}, b interface{}) bool {
			left := a.(porcupineState)
			right := b.(porcupineState)
			if left.revision != right.revision || len(left.values) != len(right.values) {
				return false
			}
			for key, leftValue := range left.values {
				if right.values[key] != leftValue {
					return false
				}
			}
			return true
		},
	}
}

func applyPorcupineOperation(st porcupineState, in porcupineInput, out porcupineOutput) (porcupineState, bool) {
	next := clonePorcupineState(st)
	switch in.kind {
	case OpPut:
		if !out.ok || out.err != "" {
			return porcupineState{}, false
		}
		next.revision++
		next.values[in.key] = modelValue{value: in.value, revision: next.revision}
		return next, out.revision == next.revision
	case OpDelete:
		_, ok := next.values[in.key]
		if !ok {
			return next, !out.ok && out.err == "NotFound"
		}
		if !out.ok || out.err != "" {
			return porcupineState{}, false
		}
		next.revision++
		delete(next.values, in.key)
		return next, out.revision == next.revision
	case OpGet:
		cur, ok := next.values[in.key]
		if !ok {
			return next, !out.ok && out.err == "NotFound"
		}
		return next, out.ok && out.err == "" && out.current == cur.value && out.revision == cur.revision
	case OpCAS:
		cur, ok := next.values[in.key]
		if !ok {
			return next, !out.ok && out.err == "NotFound"
		}
		if cur.value != in.expect {
			return next, out.ok && out.err == "" && !out.swapped && out.current == cur.value && out.revision == cur.revision
		}
		if !out.ok || out.err != "" || !out.swapped {
			return porcupineState{}, false
		}
		next.revision++
		next.values[in.key] = modelValue{value: in.value, revision: next.revision}
		return next, out.current == in.value && out.revision == next.revision
	default:
		return porcupineState{}, false
	}
}

func clonePorcupineState(st porcupineState) porcupineState {
	out := porcupineState{revision: st.revision, values: map[string]modelValue{}}
	for key, value := range st.values {
		out.values[key] = value
	}
	return out
}
