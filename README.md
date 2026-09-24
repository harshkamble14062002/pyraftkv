# PyRaftKV

PyRaftKV is an educational, fault-tolerant key-value store implemented in
Python. A static cluster uses Raft to elect a leader, replicate PUT and DELETE
commands, survive node failures, and recover durable state after restart.

The project is in v1.0 release-candidate hardening. It implements one Raft
group; it does not currently implement sharding or dynamic membership.

## Features

- Raft leader election with randomized 1.5–3 second election timeouts
- replicated PUT and DELETE commands with majority commit
- leader-only linearizable GET requests
- quorum read barriers and a conservative 500 ms read lease
- concurrent per-follower replication with bounded waits
- log repair using next_index and match_index
- durable term, vote, commit index, log journal, and snapshots
- logical log indexes after prefix compaction
- InstallSnapshot catch-up behind compacted history
- best-effort leadership transfer during planned shutdown
- deterministic partitions, directional failures, drops, and delays
- concurrent workload and recovery stress tests
- read-only diagnostics at /cluster/status
- Prometheus metrics and a provisioned Grafana dashboard
- a reproducible Docker leader-failover demonstration
- machine-readable and Markdown benchmark results

## Architecture

Clients send writes and authoritative reads to the elected leader. The leader
replicates commands and applies an entry after a majority accepts it.

~~~mermaid
flowchart LR
    Client --> API[FastAPI client API]
    API --> Leader[Raft leader]
    Leader --> Log[Raft log]
    Log --> Store[KV state machine]
    Leader --> Follower1[Raft follower]
    Leader --> Follower2[Raft follower]
    Leader --> Persistence[Durable state, journal, snapshot]
    Follower1 --> Persistence1[Durable state]
    Follower2 --> Persistence2[Durable state]
~~~

See [the architecture guide](docs/architecture.md) for component boundaries,
write/read flows, locking, persistence, and recovery. Protocol behavior is in
[the Raft guide](docs/raft.md).

## Requirements

- Python 3.12 or newer
- Docker with the Compose plugin for multi-node deployment
- Linux, macOS, or another environment capable of running the tooling

## Development setup

~~~bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e ".[dev]"
pytest
ruff check .
~~~

Run a single local node:

~~~bash
pyraftkv-node \
  --node-id node1 \
  --host 127.0.0.1 \
  --port 8000 \
  --data-dir ./data/node1
~~~

A single node elects itself after its election timeout. Use the Docker cluster
for the fault-tolerance demonstration.

## Docker quick start

~~~bash
docker compose up -d --build
docker compose ps
~~~

| Service | URL |
|---|---|
| node1 | http://127.0.0.1:8001 |
| node2 | http://127.0.0.1:8002 |
| node3 | http://127.0.0.1:8003 |
| Prometheus | http://127.0.0.1:9090 |
| Grafana | http://127.0.0.1:3000 |

Grafana uses development credentials admin / admin from docker-compose.yml.

Discover the leader rather than assuming a fixed node:

~~~bash
curl -s http://127.0.0.1:8001/cluster/status
curl -s http://127.0.0.1:8002/cluster/status
curl -s http://127.0.0.1:8003/cluster/status
~~~

The response whose role is leader identifies the client endpoint. Stop without
deleting named data volumes with docker compose down.

## Automated failover demonstration

~~~bash
./scripts/demo_failover.py
~~~

The demo validates an initial write/read, stops the discovered leader, waits for
a different leader in a newer term, writes and linearly reads with two nodes,
restarts the failed node, waits for commit/apply/log catch-up, and checks every
metrics endpoint.

All waits and Docker commands are bounded. Failures print Compose status,
recent logs, and reachable cluster status. Use --no-build to reuse images.
See [the failure-testing guide](docs/failure-testing.md).

## Client API

Replace port 8001 with the leader port discovered from /cluster/status.

~~~bash
curl -X PUT http://127.0.0.1:8001/kv/language \
  -H 'Content-Type: application/json' \
  -d '{"value":"python"}'

curl http://127.0.0.1:8001/kv/language

curl -X DELETE http://127.0.0.1:8001/kv/language
~~~

| Method | Path | Behavior |
|---|---|---|
| PUT | /kv/{key} | Replicate and commit through the leader |
| GET | /kv/{key} | Leader-only linearizable read |
| DELETE | /kv/{key} | Replicate and commit a deletion |
| GET | /health | Process and local Raft summary |
| GET | /cluster/status | Immutable operational Raft snapshot |
| GET | /metrics | Prometheus exposition |

Followers return HTTP 409 with a known leader_id. A leader returns HTTP 503
when it cannot obtain quorum. Clients must rediscover and retry; automatic
proxying is not implemented. The /raft/* paths are internal peer RPCs.

## Consistency and persistence

Writes succeed after majority commit. Followers apply entries in order.
Linearizable reads require a current-term quorum confirmation or a still-valid
short read lease.

Each persistent node directory contains:

- raft-state.json: current term, vote, and commit index;
- raft-log.json: append/truncate records or a compacted journal snapshot;
- raft-snapshot.json: versioned state and included index/term.

Normal appends are journaled rather than rewriting the whole log. A snapshot is
persisted before its covered prefix is compacted. Startup restores the
snapshot, validates the boundary, replays committed retained entries, and
always rejoins as a follower.

Snapshot creation is an explicit RaftNode operation; the runtime has no
automatic size/time-based compaction policy.

## Observability

Prometheus scrapes all nodes every two seconds. The Grafana dashboard covers
term and role, commit/apply/log/snapshot indexes, elections, leadership
changes, RPC failures and latency, follower lag, read barriers, snapshots,
compactions, and HTTP traffic.

Metric labels are bounded. Arbitrary KV keys are not labels.

## Benchmarks

~~~bash
python -m benchmarks.benchmark_raft \
  --requests 1000 \
  --concurrency 10 \
  --recovery-entries 100
~~~

The committed Phase H results are available as
[Markdown](benchmarks/results/local-raft-phase-h.md) and
[JSON](benchmarks/results/local-raft-phase-h.json).

They are local in-memory development measurements, not production or network
performance claims. See [the benchmark guide](docs/benchmarks.md).

## Testing

~~~bash
pytest
ruff check .
git diff --check
~~~

Focused failure suites:

~~~bash
pytest tests/integration/test_raft_failure_chaos.py
pytest tests/integration/test_raft_snapshot_chaos.py
pytest tests/integration/test_raft_concurrent_stress.py
pytest tests/unit/test_demo_failover.py
~~~

The chaos suite uses deterministic in-memory faults, not external timing.

## Repository layout

~~~text
src/pyraftkv/
  api/              client, admin, and peer HTTP routes
  observability/    Prometheus collectors
  raft/             consensus, persistence, and snapshots
  storage/          KV store and standalone WAL components
  transport/        in-memory and HTTP transports
benchmarks/         benchmark suites and committed results
monitoring/         Prometheus and Grafana configuration
scripts/            operational demonstrations
tests/              unit and integration coverage
~~~

## Limitations

PyRaftKV is educational, not a production database.

- static membership and peer addresses;
- one Raft group, with no sharding or consistent hashing;
- no authentication, authorization, TLS, or tenant isolation;
- no follower proxying or smart client;
- snapshots use one JSON payload rather than streamed chunks;
- no automatic snapshot/compaction scheduling;
- no backup tooling, rolling upgrades, or membership changes;
- development-default monitoring credentials;
- local benchmarks are not capacity guarantees.

## Roadmap and release status

See [the roadmap](docs/roadmap.md) and [v1.0 release notes](RELEASE_NOTES.md).
Version metadata is ready, but no v1.0.0 tag or release is created automatically;
tagging requires explicit authorization after final validation.
