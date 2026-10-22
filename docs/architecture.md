# Quorum Architecture

Quorum is a Raft-backed coordination service. Raft orders opaque commands, the
state machine applies them deterministically, and clients interact with the
leader through a KV API with sessions, CAS, leases, fencing tokens, and watches.

Current milestone:

- Pure Raft package with leader election and log replication tests.
- Append-only WAL with CRC and torn-write recovery.
- In-memory KV state machine with CAS, session dedup, revisions, and watch events.
- Health/readiness HTTP endpoints for the future server process.

Next milestone:

- Persist Raft hard state and log entries through `internal/storage`.
- Add the gRPC transport and generated protobuf bindings.
- Wire `cmd/quorumd` into a real three-node cluster.

