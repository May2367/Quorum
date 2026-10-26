# Quorum

Quorum is a Raft-based coordination service in Go. The core project is a replicated key-value store with WAL persistence, CAS, sessions, watches, and a path toward leases and fencing tokens. The flagship workload is a small settlement ledger that makes the distributed-systems guarantees concrete: workers settle idempotent money-movement jobs while duplicate retries and stale fencing tokens are rejected.

This is an undergraduate resume project, so the repository favors a clear end-to-end story over production breadth. The current code is a foundation plus a working settlement workload; replicated peer transport is now wired into the server, while full proposal tracking, lease APIs, Porcupine checks, and AWS deployment remain roadmap items.

## What Works Now

- `internal/raft`: pure Raft state machine with leader election, replication flow, quorum commit, and tests.
- `internal/storage`: append-only WAL with CRC records, fsync, torn-write recovery, and Raft hard-state/log recovery.
- `internal/statemachine`: KV state machine with CAS, session dedup, revisions, watches, plus a settlement ledger with idempotency and fencing-token checks.
- `internal/server`: async Raft driver, single-node persistent runtime, committed-log replay, health/readiness endpoints, static peer parsing, and KV gRPC handlers.
- `internal/transport`: protobuf conversion plus one-way gRPC peer delivery for Raft messages.
- `cmd/quorumd`: server with separate HTTP, client gRPC, and peer gRPC listeners.
- `cmd/quorumctl`: basic KV client CLI.
- `cmd/scheduler-demo`: settlement workload demo with 20 workers and 500 jobs.
- `deploy/docker`: initial three-node Compose wiring with static peer addresses.
- `deploy/terraform`, `test/chaos`, `test/linearizability`: scaffolding for the later deployment and verification story.

## Settlement Demo

The demo creates one escrow account, 20 merchant accounts, and 500 settlement jobs. Workers apply ledger transfers using each job ID as the idempotency key. Every worker intentionally retries the same transfer once, and the ledger rejects the duplicate. A separate stale-token path demonstrates that older fencing tokens cannot mutate the guarded ledger resource.

```sh
go run ./cmd/scheduler-demo
```

Expected output:

```text
settlement jobs=500 workers=20
completed_once=500 duplicate_retries_rejected=500 stale_fencing_rejected=true
total_cents=50200 expected_total_cents=50200 no_negative_balances=true
escrow_balance=0 transfers_recorded=501 revision=524
```

## Verify

```sh
go test ./...
go test -race ./...
```

## Run The Single-Node Server

```sh
go run ./cmd/quorumd -node-id=1 -http-addr=:8080 -client-addr=:9090 -peer-addr=:9000 -data-dir=data
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz
```

In another shell:

```sh
go run ./cmd/quorumctl put demo/key value
go run ./cmd/quorumctl get demo/key
```

## Run A Static Local Cluster

The server accepts a static peer map:

```sh
-peers=1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003
```

Docker Compose passes that map to three nodes:

```sh
docker compose -f deploy/docker/compose.yml up --build
```

The cluster wiring starts separate peer gRPC listeners and sends Raft `Ready.Messages` through the peer transport. Client proposal tracking and leader redirects are still in the roadmap, so the cluster is a replication foundation rather than the final client-facing distributed KV experience.

## Roadmap

1. Add proposal tracking that completes client requests after committed entries are applied.
2. Route the settlement ledger through replicated Raft commands.
3. Add leader redirect handling, client retries, and ReadIndex reads.
4. Add leased claims, keepalive, lock/fencing APIs, and revision-resumable watches.
5. Add Porcupine history checks, Docker chaos runs, benchmarks, and AWS Terraform deployment.

## Scope Limits

The settlement ledger is intentionally not a payment processor. There is no auth, external integration, refunds, reserve/capture flow, merchant onboarding, transaction listing, or double-entry accounting. Its job is to prove idempotent, fenced state-machine updates under the coordination service.
