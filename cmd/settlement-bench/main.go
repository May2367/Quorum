package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"quorum/client"
)

func main() {
	endpoints := flag.String("endpoints", "127.0.0.1:9090", "comma-separated quorumd client gRPC endpoints")
	transfers := flag.Int("transfers", 1000, "number of ledger transfers to run")
	concurrency := flag.Int("concurrency", 20, "number of concurrent transfer workers")
	merchants := flag.Int("merchants", 20, "number of destination merchant accounts")
	cents := flag.Int64("cents", 1, "cents per transfer")
	timeout := flag.Duration("timeout", 10*time.Second, "per-RPC timeout")
	runID := flag.String("run-id", time.Now().UTC().Format("20060102T150405.000000000"), "unique account/idempotency key prefix")
	flag.Parse()

	if *transfers < 1 || *concurrency < 1 || *merchants < 1 || *cents < 1 {
		log.Fatal("transfers, concurrency, merchants, and cents must be positive")
	}

	ctx := context.Background()
	c := client.New(client.Config{Endpoints: splitEndpoints(*endpoints), Timeout: *timeout})
	prefix := "bench/" + *runID
	escrow := prefix + "/escrow"
	initialEscrow := int64(*transfers) * *cents

	must(c.CreateAccount(ctx, escrow, initialEscrow), "create escrow")
	merchantIDs := make([]string, *merchants)
	for i := range merchantIDs {
		merchantIDs[i] = fmt.Sprintf("%s/merchant-%03d", prefix, i)
		must(c.CreateAccount(ctx, merchantIDs[i], 0), "create merchant")
	}

	jobs := make(chan int)
	latencies := make([]time.Duration, *transfers)
	var completed atomic.Int64
	var firstErr error
	var errOnce sync.Once
	var wg sync.WaitGroup
	start := time.Now()
	for worker := 0; worker < *concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				merchant := merchantIDs[i%len(merchantIDs)]
				key := fmt.Sprintf("%s/transfer-%06d", prefix, i)
				opStart := time.Now()
				_, err := c.Transfer(ctx, escrow, merchant, *cents, key, 0)
				latencies[i] = time.Since(opStart)
				if err != nil {
					errOnce.Do(func() { firstErr = err })
					continue
				}
				completed.Add(1)
			}
		}()
	}
	for i := 0; i < *transfers; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	duration := time.Since(start)
	if firstErr != nil {
		log.Fatalf("transfer failed: %v", firstErr)
	}

	total, negative := totalAndNegative(ctx, c, escrow, merchantIDs)
	completedCount := completed.Load()
	throughput := float64(completedCount) / duration.Seconds()
	fmt.Printf("settlement_benchmark endpoints=%s transfers=%d concurrency=%d merchants=%d cents=%d\n", *endpoints, *transfers, *concurrency, *merchants, *cents)
	fmt.Printf("completed=%d duration_ms=%.2f throughput_ops_sec=%.2f\n", completedCount, millis(duration), throughput)
	fmt.Printf("latency_ms p50=%.2f p95=%.2f p99=%.2f max=%.2f\n", millis(percentile(latencies, 0.50)), millis(percentile(latencies, 0.95)), millis(percentile(latencies, 0.99)), millis(maxDuration(latencies)))
	fmt.Printf("total_cents=%d expected_total_cents=%d no_negative_balances=%t\n", total, initialEscrow, !negative)
}

func totalAndNegative(ctx context.Context, c *client.Client, escrow string, merchants []string) (int64, bool) {
	accounts := append([]string{escrow}, merchants...)
	var total int64
	var negative bool
	for _, account := range accounts {
		balance, err := c.GetBalance(ctx, account)
		must(err, "get balance")
		total += balance
		if balance < 0 {
			negative = true
		}
	}
	return total, negative
}

func percentile(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func maxDuration(values []time.Duration) time.Duration {
	var max time.Duration
	for _, value := range values {
		if value > max {
			max = value
		}
	}
	return max
}

func millis(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
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

func must(err error, action string) {
	if err != nil {
		log.Fatalf("%s: %v", action, err)
	}
}
