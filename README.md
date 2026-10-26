# Quorum

Quorum is a Raft-based coordination service in Go. The core project is a replicated key-value store with WAL persistence, CAS, sessions, watches, and a path toward leases and fencing tokens. The flagship workload is a small settlement ledger that makes the distributed-systems guarantees concrete: workers settle idempotent money-movement jobs while duplicate retries and stale fencing tokens are rejected.

This is an undergraduate resume project, so the repository favors a clear end-to-end story over production breadth. The current code is a foundation plus a replicated settlement workload, seeded Raft safety tests, WAL-backed restart coverage, static peer transport wiring, public lock/ledger APIs, and client redirect support. Porcupine checks, broader chaos tests, benchmarks, and AWS deployment remain roadmap items.

## What Works Now

- `internal/raft`: pure Raft state machine with leader election, replication flow, quorum commit, and tests.
- `internal/simnet`: seeded Raft simulator with randomized delivery/reordering, message drops, partitions/heals, crash/restart from simulated durable state, and safety invariant checks.
- `internal/storage`: append-only WAL with CRC records, fsync, torn-write recovery, corrupt-tail truncation tests, and Raft hard-state/log recovery.
- `internal/statemachine`: KV state machine with CAS, session dedup, revisions, watches, plus a settlement ledger with idempotency and fencing-token checks.
- `internal/server`: async Raft driver, single-node persistent runtime, WAL-backed restart/catch-up tests, committed-log replay, health/readiness endpoints, static peer parsing, and KV/lock/ledger gRPC handlers.
- `internal/transport`: protobuf conversion plus one-way gRPC peer delivery for Raft messages.
- `cmd/quorumd`: server with separate HTTP, client gRPC, and peer gRPC listeners.
- `cmd/quorumctl`: KV, lock, and ledger client CLI.
- `cmd/scheduler-demo`: replicated settlement workload demo with 20 workers and 500 jobs.
- `deploy/docker`: initial three-node Compose wiring with static peer addresses.
- `deploy/terraform`, `test/chaos`, `test/linearizability`: scaffolding for the later deployment and verification story.

## Correctness Tests

The current safety suite checks the Raft foundation before adding more server breadth:

- 50 deterministic randomized simulator seeds.
- At most one leader per term.
- Log matching across live and durable node state.
- Committed entries and committed prefixes are never lost on nodes whose commit index includes them.
- Figure 8 style partition/heal leader churn.
- Partitioned old-leader convergence after a majority-side commit.
- WAL truncation of incomplete and corrupt trailing records while preserving the complete prefix.
- WAL-backed follower crash, restart, catch-up, and durable re-restart.

Run:

```sh
go test ./...
go test -race ./...
```

## Settlement Demo

The demo expects a running `quorumd` endpoint. It creates one escrow account, 20 merchant accounts, and 500 settlement jobs through the public replicated APIs. Workers claim each job through the lock API, use the returned fencing token for a ledger transfer, intentionally retry the same transfer once, and verify that the ledger rejects the duplicate. A separate stale-token path advances deterministic lock time, reacquires a job with a newer token, and verifies that the old token cannot mutate that job.

```sh
go run ./cmd/quorumd -node-id=1 -http-addr=:8080 -client-addr=:9090 -peer-addr=:9000 -data-dir=data
go run ./cmd/scheduler-demo -endpoints=127.0.0.1:9090
```

Expected output shape:

```text
settlement jobs=500 workers=20 endpoint=127.0.0.1:9090
completed_once=500 duplicate_retries_rejected=500 stale_fencing_rejected=true
total_cents=50200 expected_total_cents=50200 no_negative_balances=true
escrow_balance=0
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

The cluster wiring starts separate peer gRPC listeners, sends Raft `Ready.Messages` through the peer transport, and supports client redirects. The replicated settlement demo has been smoke-tested against three local `quorumd` processes. Docker/container chaos and larger multi-node workload demos are still future work.

## Roadmap

1. Add containerized settlement chaos with worker/leader kills and replayable seeds.
2. Add Porcupine or a scalable linearizability checker for larger fault-injected histories.
3. Add Docker chaos runs, benchmarks, and one optimization pass.
4. Add minimal AWS Terraform deployment and recorded cross-AZ numbers.
5. Make revision-resumable watches production-quality or leave them clearly documented as a stretch goal.

## Scope Limits

The settlement ledger is intentionally not a payment processor. There is no auth, external integration, refunds, reserve/capture flow, merchant onboarding, transaction listing, or double-entry accounting. Its job is to prove idempotent, fenced state-machine updates under the coordination service.
