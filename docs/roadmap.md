# PyRaftKV Development Roadmap

## Current status

PyRaftKV has completed the implementation and documentation phases of Issue
#46 on the production-hardening branch. The v1.0 release candidate passed final
validation on 2026-09-23. Package metadata reports 1.0.0, but no v1.0.0 tag or
published release has been created.

## Completed foundations

- thread-safe in-memory KV state machine
- standalone write-ahead log, replay, snapshots, and compaction
- Raft follower, candidate, and leader state
- randomized elections, voting rules, and heartbeats
- replicated logical log with conflict repair
- majority commit and current-term commit rule
- durable term, vote, commit index, and append-only log journal
- HTTP peer transport and FastAPI runtime
- three-node Docker deployment
- Prometheus and Grafana provisioning
- unavailable-peer liveness hardening

## Issue #46 hardening phases

| Phase | Status | Result |
|---|---|---|
| A | Complete | Quorum-confirmed leader-only linearizable reads |
| B | Complete | Conservative monotonic 500 ms read lease |
| C | Complete | Best-effort graceful leadership transfer with TimeoutNow |
| D | Complete | Durable versioned snapshots and InstallSnapshot RPC |
| E | Complete | Non-zero logical log base indexes and recovery |
| F | Complete | Deterministic partition, drop, delay, restart, and recovery tests |
| G | Complete | Concurrent PUT/GET/DELETE stress under failures |
| H | Complete | Failure and recovery benchmark suite with committed artifacts |
| I | Complete | Immutable read-only /cluster/status API |
| J | Complete | Expanded bounded-cardinality metrics and Grafana dashboard |
| K | Complete | Real Docker leader-failover and catch-up demonstration |
| L | Complete | README, architecture, Raft, failure, benchmark, and roadmap docs |
| M | Ready | v1.0 validation and release preparation; tag authorization pending |

## Phase M checklist

Validated on 2026-09-23:

- [x] update package and API version metadata to 1.0.0;
- [x] run the complete pytest suite: 263 passed;
- [x] run repository-wide Ruff;
- [x] run git diff --check;
- [x] validate docker compose config and rebuild all node images;
- [x] start all nodes, Prometheus, and Grafana;
- [x] verify leader election and normal write/read replication;
- [x] rerun the automated leader-failover demonstration;
- [x] verify the restarted node caught up through commit index 6;
- [x] inspect three healthy Prometheus targets and Grafana database health;
- [x] prepare release notes with tested limitations;
- [x] confirm the intended worktree and commit history;
- [ ] obtain explicit authorization before creating a v1.0.0 tag.

The Docker run did not compact because automatic compaction is intentionally
not implemented. InstallSnapshot catch-up and snapshot recovery passed in the
deterministic integration suite included in the 263 tests.

A tag or published release is never created automatically.

## Post-v1 possibilities

These are deliberately outside the current release scope:

- dynamic Raft membership and joint consensus
- multiple Raft groups, sharding, and request routing
- streamed or chunked snapshots
- automatic compaction policy based on log size or age
- TLS, authentication, authorization, and secret management
- first-class backup and restore workflows
- rolling-upgrade compatibility policy
- smarter clients with leader discovery and retry
- broader long-duration soak and performance testing

Future work should preserve the current emphasis on small, explicit,
well-tested distributed-systems mechanisms.
