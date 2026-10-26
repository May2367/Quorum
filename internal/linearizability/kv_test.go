package linearizability

import "testing"

func TestCheckKVAcceptsSequentialHistory(t *testing.T) {
	ops := []Operation{
		{ID: 1, Start: 1, End: 2, Kind: OpPut, Key: "x", Value: "1", OK: true, Revision: 1},
		{ID: 2, Start: 3, End: 4, Kind: OpGet, Key: "x", OK: true, Current: "1", Revision: 1},
		{ID: 3, Start: 5, End: 6, Kind: OpCAS, Key: "x", Expect: "1", Value: "2", OK: true, Swapped: true, Current: "2", Revision: 2},
		{ID: 4, Start: 7, End: 8, Kind: OpDelete, Key: "x", OK: true, Revision: 3},
		{ID: 5, Start: 9, End: 10, Kind: OpGet, Key: "x", OK: false, Err: "NotFound"},
	}
	checkAccepts(t, ops)
}

func TestCheckKVRejectsImpossibleHistory(t *testing.T) {
	ops := []Operation{
		{ID: 1, Start: 1, End: 2, Kind: OpPut, Key: "x", Value: "1", OK: true, Revision: 1},
		{ID: 2, Start: 3, End: 4, Kind: OpGet, Key: "x", OK: true, Current: "missing", Revision: 1},
	}
	checkRejects(t, ops, "expected impossible history to be rejected")
}

func TestCheckKVAcceptsOverlappingHistoryWithAlternateOrder(t *testing.T) {
	ops := []Operation{
		{ID: 1, Start: 1, End: 10, Kind: OpPut, Key: "x", Value: "1", OK: true, Revision: 1},
		{ID: 2, Start: 2, End: 3, Kind: OpGet, Key: "x", OK: false, Err: "NotFound"},
	}
	checkAccepts(t, ops)
}

func TestCheckKVRejectsRealTimeOrderViolation(t *testing.T) {
	ops := []Operation{
		{ID: 1, Start: 1, End: 2, Kind: OpPut, Key: "x", Value: "1", OK: true, Revision: 1},
		{ID: 2, Start: 3, End: 4, Kind: OpGet, Key: "x", OK: false, Err: "NotFound"},
	}
	checkRejects(t, ops, "expected real-time order violation to be rejected")
}

func checkAccepts(t *testing.T, ops []Operation) {
	t.Helper()
	if err := CheckKV(ops); err != nil {
		t.Fatalf("local checker rejected history: %v", err)
	}
	if err := CheckKVPorcupine(ops); err != nil {
		t.Fatalf("porcupine rejected history: %v", err)
	}
}

func checkRejects(t *testing.T, ops []Operation, message string) {
	t.Helper()
	if err := CheckKV(ops); err == nil {
		t.Fatal(message + " with local checker")
	}
	if err := CheckKVPorcupine(ops); err == nil {
		t.Fatal(message + " with porcupine")
	}
}
