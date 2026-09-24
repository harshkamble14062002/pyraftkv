# PyRaftKV 1.0.0 Release Notes

PyRaftKV 1.0.0 is the first release candidate for the project's complete
single-group Raft implementation. It focuses on correctness, durable recovery,
failure handling, observability, and reproducible validation.

No `v1.0.0` tag or published release has been created. Tagging requires
explicit authorization after the final validation checklist passes.

## Highlights

- leader election, heartbeats, replicated PUT and DELETE commands, majority
  commit, conflict repair, and ordered state-machine application;
- leader-only linearizable reads using quorum barriers and a conservative
  short read lease;
- best-effort leadership transfer during planned shutdown;
- durable term, vote, commit index, append-only log journal, versioned
  snapshots, logical indexes after compaction, and restart recovery;
- InstallSnapshot catch-up when a follower is behind compacted history;
- deterministic partition, delay, drop, restart, snapshot, and concurrent
  workload tests;
- read-only `/cluster/status` diagnostics, bounded-cardinality Prometheus
  metrics, and a provisioned Grafana dashboard;
- reproducible benchmarks and an automated Docker leader-failover demo.

## Compatibility

The client API remains `PUT`, `GET`, and `DELETE /kv/{key}`. Followers still
return HTTP 409 with the known leader ID, and a leader unable to confirm quorum
returns HTTP 503. Persistent state formats remain versioned and compatible
with the formats introduced during pre-release development.

The package requires Python 3.12 or newer.

## Validation

Validated on 2026-09-23:

- 263 pytest tests passed, and repository-wide Ruff and Git whitespace checks
  passed;
- Docker Compose configuration, image build, and five-service startup passed;
- real three-node leader failover, quorum write/read, restart, and catch-up
  through commit index 6 passed;
- all three Prometheus targets were healthy and Grafana reported database OK.

Snapshot installation and recovery are exercised by deterministic integration
tests. The Docker runtime does not compact automatically, so Docker snapshot
catch-up is applicable only when a snapshot has been created explicitly.

## Known limitations

- educational implementation, not a production database;
- static membership and a single Raft group;
- no sharding, dynamic membership, or joint consensus;
- no authentication, authorization, TLS, or tenant isolation;
- no automatic follower proxying or leader-aware client;
- snapshots are single JSON payloads and compaction is not scheduled
  automatically;
- no backup tooling or rolling-upgrade compatibility policy;
- Docker monitoring uses development-default Grafana credentials;
- committed local benchmarks are not capacity or production performance
  guarantees.

Operational details are in the [README](README.md),
[architecture guide](docs/architecture.md), [Raft guide](docs/raft.md), and
[failure-testing guide](docs/failure-testing.md).
