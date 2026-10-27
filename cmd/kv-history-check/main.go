package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"quorum/client"
	"quorum/internal/linearizability"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	endpoints := flag.String("endpoints", "127.0.0.1:9090", "comma-separated quorumd client gRPC endpoints")
	key := flag.String("key", "history/acct", "KV key to exercise")
	seed := flag.Int64("seed", 1, "deterministic operation seed")
	rounds := flag.Int("rounds", 4, "number of concurrent operation rounds")
	timeout := flag.Duration("timeout", 5*time.Second, "per-RPC timeout")
	opTimeout := flag.Duration("op-timeout", 15*time.Second, "maximum wall time for each recorded operation, including retries")
	flag.Parse()
	if *rounds < 1 {
		log.Fatal("rounds must be positive")
	}

	clients := []*client.Client{
		client.New(client.Config{Endpoints: splitEndpoints(*endpoints), Timeout: *timeout}),
		client.New(client.Config{Endpoints: reverse(splitEndpoints(*endpoints)), Timeout: *timeout}),
	}
	var clock atomic.Int64
	history := make([]linearizability.Operation, 0, 1+(*rounds*3))
	initOp := recordPut(&clock, clients[0], *key, "0", fmt.Sprintf("history-%d-init", *seed), 1, *opTimeout)
	initOp.ID = 1
	revisionOffset := uint64(0)
	if initOp.OK && initOp.Revision > 0 {
		revisionOffset = initOp.Revision - 1
	}
	normalizeRevision(&initOp, revisionOffset)
	history = append(history, initOp)
	check(history)

	rng := rand.New(rand.NewSource(*seed))
	expected := "0"
	next := 1
	for round := 0; round < *rounds; round++ {
		ops := make([]func() linearizability.Operation, 0, 3)
		if rng.Intn(2) == 0 {
			clientIndex := rng.Intn(len(clients))
			ops = append(ops, func() linearizability.Operation {
				return recordGet(&clock, clients[clientIndex], *key, *opTimeout)
			})
		}
		from := expected
		to := fmt.Sprintf("%d", next)
		clientID := fmt.Sprintf("history-%d-cas-%d", *seed, round)
		casClientIndex := rng.Intn(len(clients))
		ops = append(ops, func() linearizability.Operation {
			return recordCAS(&clock, clients[casClientIndex], *key, from, to, clientID, 1, *opTimeout)
		})
		if rng.Intn(2) == 0 {
			clientIndex := rng.Intn(len(clients))
			ops = append(ops, func() linearizability.Operation {
				return recordGet(&clock, clients[clientIndex], *key, *opTimeout)
			})
		}
		recorded := runOps(ops)
		for i := range recorded {
			recorded[i].ID = len(history) + i + 1
			normalizeRevision(&recorded[i], revisionOffset)
		}
		history = append(history, recorded...)
		check(history)
		expected = to
		next++
	}

	fmt.Printf("kv_history_check seed=%d rounds=%d operations=%d result=pass\n", *seed, *rounds, len(history))
}

func retryUntil[T any](timeout time.Duration, fn func() (T, error)) (T, error) {
	deadline := time.Now().Add(timeout)
	for {
		res, err := fn()
		if err == nil || isSemanticKVError(err) || time.Now().After(deadline) {
			return res, err
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func isSemanticKVError(err error) bool {
	return status.Code(err) == codes.NotFound
}

func normalizeRevision(op *linearizability.Operation, offset uint64) {
	if !op.OK || op.Revision == 0 || offset == 0 {
		return
	}
	if op.Revision <= offset {
		op.Revision = 0
		return
	}
	op.Revision -= offset
}

func runOps(ops []func() linearizability.Operation) []linearizability.Operation {
	var wg sync.WaitGroup
	recorded := make([]linearizability.Operation, len(ops))
	for i, op := range ops {
		i, op := i, op
		wg.Add(1)
		go func() {
			defer wg.Done()
			recorded[i] = op()
		}()
	}
	wg.Wait()
	return recorded
}

func recordPut(clock *atomic.Int64, c *client.Client, key, value, clientID string, sequence uint64, timeout time.Duration) linearizability.Operation {
	start := clock.Add(1)
	revision, err := retryUntil(timeout, func() (uint64, error) {
		return c.Put(context.Background(), key, value, clientID, sequence)
	})
	end := clock.Add(1)
	op := linearizability.Operation{Start: start, End: end, Kind: linearizability.OpPut, Key: key, Value: value}
	if err != nil {
		op.Err = clientCodeName(err)
		return op
	}
	op.OK = true
	op.Revision = revision
	return op
}

func recordGet(clock *atomic.Int64, c *client.Client, key string, timeout time.Duration) linearizability.Operation {
	start := clock.Add(1)
	type getResult struct {
		value    string
		revision uint64
	}
	res, err := retryUntil(timeout, func() (getResult, error) {
		value, revision, err := c.Get(context.Background(), key)
		return getResult{value: value, revision: revision}, err
	})
	end := clock.Add(1)
	op := linearizability.Operation{Start: start, End: end, Kind: linearizability.OpGet, Key: key}
	if err != nil {
		op.Err = clientCodeName(err)
		return op
	}
	op.OK = true
	op.Current = res.value
	op.Revision = res.revision
	return op
}

func recordCAS(clock *atomic.Int64, c *client.Client, key, expect, value, clientID string, sequence uint64, timeout time.Duration) linearizability.Operation {
	start := clock.Add(1)
	res, err := retryUntil(timeout, func() (client.CASResult, error) {
		return c.CompareAndSwap(context.Background(), key, expect, value, clientID, sequence)
	})
	end := clock.Add(1)
	op := linearizability.Operation{Start: start, End: end, Kind: linearizability.OpCAS, Key: key, Expect: expect, Value: value}
	if err != nil {
		op.Err = clientCodeName(err)
		return op
	}
	op.OK = true
	op.Swapped = res.Swapped
	op.Current = res.Current
	op.Revision = res.Revision
	return op
}

func check(history []linearizability.Operation) {
	if err := linearizability.CheckKV(history); err != nil {
		log.Fatalf("local checker failed: %v", err)
	}
	if err := linearizability.CheckKVPorcupine(history); err != nil {
		log.Fatalf("porcupine failed: %v", err)
	}
}

func clientCodeName(err error) string {
	if status.Code(err) == codes.NotFound {
		return "NotFound"
	}
	return status.Code(err).String()
}

func splitEndpoints(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func reverse(in []string) []string {
	out := append([]string(nil), in...)
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}
