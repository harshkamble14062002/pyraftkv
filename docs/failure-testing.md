# Failure and Recovery Testing

## Test strategy

PyRaftKV separates fast deterministic consensus testing from the real Docker
demonstration.

The primary failure and stress suites use InMemoryTransport. They do not depend
on DNS, container scheduling, external services, or random packet loss.
Docker validation is retained as an operational end-to-end check.

## Deterministic transport controls

InMemoryTransport supports:

| Control | Effect |
|---|---|
| block(node) | Makes one node unreachable in both directions |
| block_link(source, target) | Blocks one directional link |
| partition(group, group, ...) | Creates symmetric isolation between groups |
| heal_partition() | Removes partition links |
| drop_next(rpc, source, target) | Drops a bounded number of selected RPCs |
| delay(rpc, source, target) | Gates an RPC until an Event is released |
| heal() | Clears faults and releases delayed RPCs |
| call_count(...) | Reports deterministic RPC observations |

Convenience methods exist for RequestVote, AppendEntries, InstallSnapshot, and
TimeoutNow drops and delays. Faults surface to production Raft code as
TransportError rather than special consensus branches.

RaftCluster in tests/helpers/raft_cluster.py adds deterministic election,
restart, bounded replication rounds, convergence checks, and invariant
diagnostics.

## Covered failure scenarios

tests/integration/test_raft_failure_chaos.py covers:

- progress with one unavailable follower;
- an isolated leader unable to commit or read after lease expiry;
- a three-of-five majority partition controlling progress;
- repair of divergent durable logs after healing;
- delayed votes and appends without cluster-wide stalls;
- durable step-down on higher-term replication, read, and snapshot responses;
- follower restart and catch-up;
- former-leader restart as follower;
- successful and failed leadership transfers.

tests/integration/test_raft_snapshot_chaos.py covers restart during snapshot
catch-up and retry after a failed snapshot transfer without partial state.

tests/integration/test_raft_concurrent_stress.py covers concurrent unique PUTs,
seeded mixed PUT/GET/DELETE traffic, follower failure, leader loss, and snapshot
catch-up while clients are active.

Run the focused suites:

~~~bash
pytest -q tests/integration/test_raft_failure_chaos.py
pytest -q tests/integration/test_raft_snapshot_chaos.py
pytest -q tests/integration/test_raft_concurrent_stress.py
~~~

## Docker failover demonstration

Run from the repository root:

~~~bash
./scripts/demo_failover.py
~~~

Options:

~~~text
--timeout SECONDS         timeout for each health/election/catch-up wait
--poll-interval SECONDS   status polling interval
--stable-samples COUNT    consecutive identical leader observations
--no-build                reuse existing node images
~~~

The script:

1. checks Docker and docker compose config;
2. starts node1, node2, and node3;
3. waits for all health endpoints;
4. requires multiple identical leader/term observations;
5. writes and reads through the discovered leader;
6. stops that exact service;
7. waits for a different leader in a newer term;
8. writes and linearly reads with two nodes;
9. verifies data committed before the failure;
10. restarts the stopped node;
11. waits for commit_index, last_applied, and log_last_index catch-up;
12. performs a final leader read and checks all metrics endpoints.

The script never assumes node1 is leader and never deletes data volumes. Every
wait and Docker subprocess is bounded. If a failure occurs after a leader was
stopped, the script attempts to restart it before exiting.

Phase K was validated successfully against the real Compose cluster during the
v1.0 hardening work.

## Diagnostics

On failure the demo prints:

- docker compose ps;
- the latest 80 log lines from all Raft nodes;
- every reachable /cluster/status payload;
- the step-specific error and a nonzero exit code.

Useful manual checks:

~~~bash
docker compose ps
docker compose logs --tail 100 node1 node2 node3

curl -s http://127.0.0.1:8001/cluster/status
curl -s http://127.0.0.1:8002/cluster/status
curl -s http://127.0.0.1:8003/cluster/status
~~~

A follower returning HTTP 409 for /kv operations is expected. Find the response
with role=leader before issuing client requests.

## Environment isolation notes

In Flatpak or containerized development tools, the Docker client or daemon
socket may not be visible even when Docker works on the host. Run the demo in a
normal host terminal. Environments that intentionally expose host commands may
use their supported host bridge.

Do not work around socket visibility by weakening Docker socket permissions.

## Cleanup

Keep durable cluster data:

~~~bash
docker compose down
~~~

Stop only the monitoring and nodes while retaining volumes:

~~~bash
docker compose stop
~~~

Volume deletion is destructive and is not part of the demonstration.
