# Quorum

Quorum is a Raft-based coordination service in Go. The core project is a replicated key-value store with WAL persistence, CAS, sessions, leader redirects, watches, locks/leases, and fencing tokens. The flagship workload is a small settlement ledger that makes the distributed-systems guarantees concrete: workers settle idempotent money-movement jobs through replicated lock and ledger APIs while duplicate retries, stale fencing tokens, and overdrafts are rejected.

The current code includes a replicated KV service, CRC-checked WAL persistence, a replicated lock/ledger workload, seeded Raft safety tests, Porcupine-backed KV history checks, process-based failover/restart measurements, Docker Compose settlement chaos, and Docker `tc netem` validation performed on a Docker-capable VM.

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
- `internal/linearizability`: KV operation-history model with local and Porcupine checkers.
- `cmd/kv-history-check`: replayable KV history generator/checker used by Docker netem chaos.
- `deploy/docker`: three-node Compose wiring with static peer addresses, plus a netem-enabled runtime target.
- `test/chaos`: process-based settlement, failover, restart-recovery scripts, plus Docker-oriented chaos entry points.
- `deploy/terraform`: AWS Terraform scaffold for a three-node EC2 topology.

## Correctness Tests

The current safety suite checks the Raft foundation, persistent runtime behavior, replicated APIs, settlement invariants, and KV linearizability histories:

- 50 deterministic randomized simulator seeds.
- At most one leader per term.
- Log matching across live and durable node state.
- Committed entries and committed prefixes are never lost on nodes whose commit index includes them.
- Figure 8 style partition/heal leader churn.
- Partitioned old-leader convergence after a majority-side commit.
- WAL truncation of incomplete and corrupt trailing records while preserving the complete prefix.
- WAL-backed follower crash, restart, catch-up, and durable re-restart.
- Initial Porcupine-backed KV history checks, including multi-node leader-failover and replayable seeded partition histories.

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

## Benchmark Settlement Writes

The benchmark command creates unique ledger accounts for each run, executes a fixed number of ledger transfers, and reports write throughput plus p50/p95/p99 latency.

```sh
go run ./cmd/settlement-bench -endpoints=127.0.0.1:9090 -transfers=1000 -concurrency=20
```

Example result table:

| target | transfers | concurrency | throughput ops/sec | p50 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| local single-node | 200 | 10 | 209.50 | 45.56 | 92.53 |
| local three-process | 200 | 10 | 66.42 | 149.62 | 214.21 |

Optimization measurement, local single-node, 200 transfers at concurrency 10:

| change | throughput ops/sec | p50 ms | p99 ms |
| --- | ---: | ---: | ---: |
| before WAL Ready batching | 329.19 | 27.96 | 70.67 |
| after WAL Ready batching | 574.62 | 15.05 | 40.19 |

This batches the Raft entries and hard state from a single Ready into one WAL fsync while preserving the persist-before-send/apply ordering.

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
docker compose -f deploy/docker/compose.yml up --build quorum1 quorum2 quorum3
```

Run the containerized settlement leader-kill smoke workflow:

```sh
deploy/scripts/docker-settlement-chaos.sh 1
```

The cluster wiring starts separate peer gRPC listeners, sends Raft `Ready.Messages` through the peer transport, and supports client redirects. The replicated settlement demo has been smoke-tested against three local `quorumd` processes, and `test/chaos/process-settlement-seeds.sh` runs replayable process-based leader-kill smoke seeds for Docker-free environments. The Docker settlement workflow builds a tools image with `quorumctl` and `scheduler-demo`, runs the 20-worker / 500-job settlement workload inside the Compose network, kills `quorum1`, and verifies a post-kill write through the surviving nodes.

The Docker `tc netem` workflow was validated on a separate Docker-capable LoomAI/FABRIC VM because this workspace cannot run Docker directly. It injects 250 ms delay and 15% packet loss on `quorum1`, runs Porcupine-checked KV histories through all three endpoints, clears the qdisc, and verifies a post-heal write. Published validation currently covers seeds 1-3.

The AWS Terraform under `deploy/terraform` is scaffold-only and has not been applied or validated against real AWS resources.

## Scope Limits

The settlement ledger is intentionally not a payment processor. There is no auth, external integration, refunds, reserve/capture flow, merchant onboarding, transaction listing, or double-entry accounting. Its job is to prove idempotent, fenced state-machine updates under the coordination service.
