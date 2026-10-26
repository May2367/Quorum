package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"quorum/client"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	workerCount = 20
	jobCount    = 500
	jobCents    = int64(100)
)

type settlementJob struct {
	id       string
	merchant string
	cents    int64
}

func main() {
	endpoints := flag.String("endpoints", "127.0.0.1:9090", "comma-separated quorumd client gRPC endpoints")
	timeout := flag.Duration("timeout", 10*time.Second, "per-RPC timeout")
	flag.Parse()

	ctx := context.Background()
	c := client.New(client.Config{Endpoints: splitEndpoints(*endpoints), Timeout: *timeout})
	initialEscrow := int64(jobCount) * jobCents
	createAccount(ctx, c, "escrow", initialEscrow)
	for i := 0; i < workerCount; i++ {
		createAccount(ctx, c, merchantID(i), 0)
	}

	jobs := make(chan settlementJob)
	var completed atomic.Int64
	var duplicates atomic.Int64
	var wg sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			holder := fmt.Sprintf("worker-%02d", worker)
			for job := range jobs {
				lock, err := c.AcquireLock(ctx, "settlement/"+job.id, holder, 10)
				if err != nil {
					log.Fatalf("claim %s: %v", job.id, err)
				}
				_, err = c.Transfer(ctx, "escrow", job.merchant, job.cents, job.id, lock.FencingToken)
				if err != nil {
					log.Fatalf("settle %s: %v", job.id, err)
				}
				completed.Add(1)

				_, err = c.Transfer(ctx, "escrow", job.merchant, job.cents, job.id, lock.FencingToken)
				if isFailedPrecondition(err) {
					duplicates.Add(1)
				} else if err != nil {
					log.Fatalf("duplicate settle %s: %v", job.id, err)
				} else {
					log.Fatalf("duplicate settle %s was accepted", job.id)
				}
			}
		}()
	}

	for i := 0; i < jobCount; i++ {
		jobs <- settlementJob{
			id:       fmt.Sprintf("settlement-%03d", i),
			merchant: merchantID(i % workerCount),
			cents:    jobCents,
		}
	}
	close(jobs)
	wg.Wait()

	staleRejected := demonstrateFencing(ctx, c)
	escrowBalance := balance(ctx, c, "escrow")
	total, negative := totalAndNegative(ctx, c)
	expectedTotal := initialEscrow + 200

	fmt.Printf("settlement jobs=%d workers=%d endpoint=%s\n", jobCount, workerCount, *endpoints)
	fmt.Printf("completed_once=%d duplicate_retries_rejected=%d stale_fencing_rejected=%t\n", completed.Load(), duplicates.Load(), staleRejected)
	fmt.Printf("total_cents=%d expected_total_cents=%d no_negative_balances=%t\n", total, expectedTotal, !negative)
	fmt.Printf("escrow_balance=%d\n", escrowBalance)
}

func createAccount(ctx context.Context, c *client.Client, accountID string, initialCents int64) {
	if err := c.CreateAccount(ctx, accountID, initialCents); err != nil && !isFailedPrecondition(err) {
		log.Fatalf("create account %s: %v", accountID, err)
	}
}

func demonstrateFencing(ctx context.Context, c *client.Client) bool {
	createAccount(ctx, c, "fence/escrow", 200)
	createAccount(ctx, c, "fence/merchant", 0)
	first, err := c.AcquireLock(ctx, "fence/job", "fence-worker-1", 1)
	if err != nil {
		log.Fatalf("first fence lock: %v", err)
	}
	if _, err := c.AdvanceLock(ctx, 1); err != nil {
		log.Fatalf("advance lock clock: %v", err)
	}
	second, err := c.AcquireLock(ctx, "fence/job", "fence-worker-2", 5)
	if err != nil {
		log.Fatalf("second fence lock: %v", err)
	}
	if second.FencingToken <= first.FencingToken {
		log.Fatalf("second fence token = %d, want > %d", second.FencingToken, first.FencingToken)
	}
	if _, err := c.Transfer(ctx, "fence/escrow", "fence/merchant", 100, "fence-job", second.FencingToken); err != nil && !isFailedPrecondition(err) {
		log.Fatalf("new-owner fenced transfer: %v", err)
	}
	if _, err := c.Transfer(ctx, "fence/escrow", "fence/merchant", 50, "fence-job", first.FencingToken); err != nil {
		return isFailedPrecondition(err)
	}
	return false
}

func totalAndNegative(ctx context.Context, c *client.Client) (int64, bool) {
	accounts := []string{"escrow", "fence/escrow", "fence/merchant"}
	for i := 0; i < workerCount; i++ {
		accounts = append(accounts, merchantID(i))
	}
	var total int64
	var negative bool
	for _, account := range accounts {
		b := balance(ctx, c, account)
		total += b
		if b < 0 {
			negative = true
		}
	}
	return total, negative
}

func balance(ctx context.Context, c *client.Client, accountID string) int64 {
	balance, err := c.GetBalance(ctx, accountID)
	if err != nil {
		log.Fatalf("balance %s: %v", accountID, err)
	}
	return balance
}

func isFailedPrecondition(err error) bool {
	if err == nil {
		return false
	}
	if status.Code(err) == codes.FailedPrecondition {
		return true
	}
	return false
}

func merchantID(i int) string {
	return fmt.Sprintf("merchant-%02d", i)
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
