package main

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"quorum/internal/statemachine"
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
	ledger := statemachine.NewLedger()
	initialEscrow := int64(jobCount) * jobCents
	must(ledger.CreateAccount("escrow", initialEscrow))
	for i := 0; i < workerCount; i++ {
		must(ledger.CreateAccount(merchantID(i), 0))
	}

	jobs := make(chan settlementJob)
	var completed atomic.Int64
	var duplicates atomic.Int64
	var wg sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				_, err := ledger.Transfer("escrow", job.merchant, job.cents, job.id, 0)
				if err != nil {
					log.Fatalf("settle %s: %v", job.id, err)
				}
				completed.Add(1)

				_, err = ledger.Transfer("escrow", job.merchant, job.cents, job.id, 0)
				if errors.Is(err, statemachine.ErrDuplicateTransfer) {
					duplicates.Add(1)
					continue
				}
				if err != nil {
					log.Fatalf("duplicate settle %s: %v", job.id, err)
				}
				log.Fatalf("duplicate settle %s was accepted", job.id)
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

	staleRejected := demonstrateFencing(ledger)
	snap := ledger.Snapshot()
	total, negative := totalAndNegative(snap.Accounts)
	expectedTotal := initialEscrow + 200

	fmt.Printf("settlement jobs=%d workers=%d\n", jobCount, workerCount)
	fmt.Printf("completed_once=%d duplicate_retries_rejected=%d stale_fencing_rejected=%t\n", completed.Load(), duplicates.Load(), staleRejected)
	fmt.Printf("total_cents=%d expected_total_cents=%d no_negative_balances=%t\n", total, expectedTotal, !negative)
	fmt.Printf("escrow_balance=%d transfers_recorded=%d revision=%d\n", snap.Accounts["escrow"], snap.Transfers, snap.Revision)
}

func demonstrateFencing(ledger *statemachine.Ledger) bool {
	must(ledger.CreateAccount("fence/escrow", 200))
	must(ledger.CreateAccount("fence/merchant", 0))
	if _, err := ledger.Transfer("fence/escrow", "fence/merchant", 100, "fence-current", 20); err != nil {
		log.Fatalf("current fenced transfer: %v", err)
	}
	_, err := ledger.Transfer("fence/escrow", "fence/merchant", 50, "fence-stale", 19)
	return errors.Is(err, statemachine.ErrStaleFencingToken)
}

func merchantID(i int) string {
	return fmt.Sprintf("merchant-%02d", i)
}

func totalAndNegative(accounts map[string]int64) (int64, bool) {
	var total int64
	var negative bool
	for _, balance := range accounts {
		total += balance
		if balance < 0 {
			negative = true
		}
	}
	return total, negative
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
