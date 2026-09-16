# PyRaftKV Development Roadmap

## Sprint 0 — Engineering Setup

- GitHub repository
- GitHub Project
- Kanban board
- Python project structure
- pytest
- Ruff
- GitHub Actions CI
- architecture documentation
- development roadmap

## Sprint 1 — Single Node KV Store

- KVStore implementation
- PUT
- GET
- DELETE
- FastAPI endpoints
- unit tests
- integration tests

Release: v0.1.0

## Sprint 2 — Persistence

- WAL format
- WAL append
- fsync
- crash recovery
- WAL replay
- snapshots

Release: v0.2.0

## Sprint 3 — Raft Leader Election

- follower state
- candidate state
- leader state
- election timeout
- RequestVote RPC
- voting rules
- heartbeats

Release: v0.3.0

## Sprint 4 — Raft Log Replication

- AppendEntries RPC
- replicated log
- quorum
- commit index
- follower synchronization
- conflict repair

Release: v0.4.0

## Sprint 5 — Fault Tolerance

- leader crash recovery
- follower restart
- stale node recovery
- network partition testing
- chaos testing

Release: v0.5.0

## Sprint 6 — Sharding

- consistent hashing
- virtual nodes
- routing
- multi-shard architecture

Release: v0.6.0

## Sprint 7 — Production Readiness

- Prometheus metrics
- structured logs
- benchmarks
- latency measurements
- throughput tests
- demo video
- final documentation

Release: v1.0.0