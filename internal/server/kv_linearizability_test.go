package server

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"quorum/internal/linearizability"
	pb "quorum/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestKVServiceConcurrentHistoryIsLinearizable(t *testing.T) {
	svc, closeFn := newTestKVService(t)
	defer closeFn()

	var clock atomic.Int64
	var nextID atomic.Int64
	var mu sync.Mutex
	var history []linearizability.Operation
	record := func(op linearizability.Operation) {
		mu.Lock()
		defer mu.Unlock()
		history = append(history, op)
	}

	run := func(fn func() linearizability.Operation) {
		op := fn()
		op.ID = int(nextID.Add(1))
		record(op)
	}

	var wg sync.WaitGroup
	calls := []func() linearizability.Operation{
		func() linearizability.Operation { return recordPut(&clock, svc, "acct", "0", "c1", 1) },
		func() linearizability.Operation { return recordCAS(&clock, svc, "acct", "0", "10", "c2", 1) },
		func() linearizability.Operation { return recordCAS(&clock, svc, "acct", "10", "20", "c3", 1) },
		func() linearizability.Operation { return recordGet(&clock, svc, "acct") },
		func() linearizability.Operation { return recordCAS(&clock, svc, "acct", "missing", "30", "c4", 1) },
		func() linearizability.Operation { return recordDelete(&clock, svc, "acct", "c5", 1) },
		func() linearizability.Operation { return recordGet(&clock, svc, "acct") },
		func() linearizability.Operation { return recordPut(&clock, svc, "acct", "40", "c6", 1) },
	}
	for _, call := range calls {
		wg.Add(1)
		go func(call func() linearizability.Operation) {
			defer wg.Done()
			run(call)
		}(call)
	}
	wg.Wait()

	if err := linearizability.CheckKV(history); err != nil {
		t.Fatalf("history is not linearizable: %v\nhistory=%+v", err, history)
	}
}

func recordPut(clock *atomic.Int64, svc *KVService, key, value, clientID string, sequence uint64) linearizability.Operation {
	start := clock.Add(1)
	res, err := svc.Put(context.Background(), &pb.PutRequest{Key: key, Value: value, ClientId: clientID, Sequence: sequence})
	end := clock.Add(1)
	op := linearizability.Operation{Start: start, End: end, Kind: linearizability.OpPut, Key: key, Value: value}
	if err != nil {
		op.Err = codeName(err)
		return op
	}
	op.OK = true
	op.Revision = res.GetRevision()
	return op
}

func recordGet(clock *atomic.Int64, svc *KVService, key string) linearizability.Operation {
	start := clock.Add(1)
	res, err := svc.Get(context.Background(), &pb.GetRequest{Key: key})
	end := clock.Add(1)
	op := linearizability.Operation{Start: start, End: end, Kind: linearizability.OpGet, Key: key}
	if err != nil {
		op.Err = codeName(err)
		return op
	}
	op.OK = true
	op.Current = res.GetValue()
	op.Revision = res.GetRevision()
	return op
}

func recordDelete(clock *atomic.Int64, svc *KVService, key, clientID string, sequence uint64) linearizability.Operation {
	start := clock.Add(1)
	res, err := svc.Delete(context.Background(), &pb.DeleteRequest{Key: key, ClientId: clientID, Sequence: sequence})
	end := clock.Add(1)
	op := linearizability.Operation{Start: start, End: end, Kind: linearizability.OpDelete, Key: key}
	if err != nil {
		op.Err = codeName(err)
		return op
	}
	op.OK = true
	op.Revision = res.GetRevision()
	return op
}

func recordCAS(clock *atomic.Int64, svc *KVService, key, expect, value, clientID string, sequence uint64) linearizability.Operation {
	start := clock.Add(1)
	res, err := svc.CompareAndSwap(context.Background(), &pb.CompareAndSwapRequest{Key: key, Expected: expect, Value: value, ClientId: clientID, Sequence: sequence})
	end := clock.Add(1)
	op := linearizability.Operation{Start: start, End: end, Kind: linearizability.OpCAS, Key: key, Expect: expect, Value: value}
	if err != nil {
		op.Err = codeName(err)
		return op
	}
	op.OK = true
	op.Swapped = res.GetSwapped()
	op.Current = res.GetCurrent()
	op.Revision = res.GetRevision()
	return op
}

func codeName(err error) string {
	if status.Code(err) == codes.NotFound {
		return "NotFound"
	}
	return status.Code(err).String()
}

func TestKVServiceRandomSequentialHistoriesAreLinearizable(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			svc, closeFn := newTestKVService(t)
			defer closeFn()

			rng := rand.New(rand.NewSource(seed))
			var clock atomic.Int64
			var history []linearizability.Operation
			seq := map[string]uint64{}
			for i := 0; i < 12; i++ {
				clientID := fmt.Sprintf("seed-%d-client-%d", seed, i)
				seq[clientID]++
				switch rng.Intn(4) {
				case 0:
					history = append(history, recordPut(&clock, svc, "acct", fmt.Sprintf("v%d", rng.Intn(5)), clientID, seq[clientID]))
				case 1:
					history = append(history, recordGet(&clock, svc, "acct"))
				case 2:
					history = append(history, recordCAS(&clock, svc, "acct", fmt.Sprintf("v%d", rng.Intn(5)), fmt.Sprintf("v%d", rng.Intn(5)+10), clientID, seq[clientID]))
				case 3:
					history = append(history, recordDelete(&clock, svc, "acct", clientID, seq[clientID]))
				}
			}
			if err := linearizability.CheckKV(history); err != nil {
				t.Fatalf("history is not linearizable: %v\nhistory=%+v", err, history)
			}
		})
	}
}
