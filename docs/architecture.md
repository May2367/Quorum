# Quorum Architecture

Quorum is a Raft-backed coordination service. Raft orders opaque commands, the
state machine applies them deterministically, and clients interact with the
leader through a KV API with sessions, CAS, leases, fencing tokens, and watches.

Quorum is organized around a small Raft core, deterministic state machines, and thin server/transport layers:

- `internal/raft` implements leader election, log replication, quorum commit, and Ready-style output.
- `internal/storage` persists Raft hard state and log records in a CRC-checked append-only WAL with fsync and corrupt-tail recovery.
- `internal/server` wires the persistent Raft runtime to KV, lock, and ledger gRPC handlers, committed-log replay, leader redirects, and HTTP health/readiness endpoints.
- `internal/transport` provides protobuf conversion and gRPC peer delivery for Raft messages.
- `internal/statemachine` contains deterministic KV, lock/fencing, and settlement ledger state machines.
- `cmd/quorumd` runs separate HTTP, client gRPC, and peer gRPC listeners.
- Static three-node clusters are configured through explicit peer maps.
