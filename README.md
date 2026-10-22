# Quorum

Quorum is a Raft-based coordination service project outlined in `plan.txt`.
The target system is a replicated key-value store with CAS, leases, fencing
tokens, watches, a job-scheduler workload, a booking stress test, Docker chaos
tests, and AWS deployment.

This repository currently contains the first working foundation:

- `internal/raft`: pure Raft state machine with election and replication tests.
- `internal/storage`: append-only WAL with CRC and torn-write recovery.
- `internal/statemachine`: KV apply layer with CAS, session dedup, revisions, and watch events.
- `internal/server`: health and readiness endpoints.
- `cmd/quorumd`, `cmd/quorumctl`, `cmd/scheduler-demo`, `cmd/booking-test`: executable entry points.
- `deploy/docker`: initial multi-stage container and three-node compose file.
- `proto/quorum.proto`: intended external API contract.

## Verify

```sh
go test ./...
go test -race ./...
```

## Run the Server Stub

```sh
go run ./cmd/quorumd -node-id=1 -client-addr=:8080 -peer-addr=:9000 -data-dir=data
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz
```

## Roadmap

1. Persist Raft hard state and log entries with `internal/storage`.
2. Generate protobuf bindings and implement the gRPC client/peer transports.
3. Wire `quorumd` into a real static three-node cluster.
4. Add leases, fencing tokens, resumable watches, and Porcupine histories.
5. Build the scheduler and booking workloads, then Docker/AWS chaos runs.

