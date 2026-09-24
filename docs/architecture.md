# PyRaftKV Architecture

## System boundary

PyRaftKV is one statically configured Raft group. Every node contains the same
consensus, persistence, API, and key-value state-machine components. One node
is leader for a term; the others are followers.

~~~mermaid
flowchart TB
    Client[Client]
    subgraph Cluster[PyRaftKV Raft group]
        N1[node1 :8001]
        N2[node2 :8002]
        N3[node3 :8003]
    end
    Client -->|PUT GET DELETE| N1
    N1 <-->|RequestVote AppendEntries InstallSnapshot TimeoutNow| N2
    N1 <-->|RequestVote AppendEntries InstallSnapshot TimeoutNow| N3
    N2 <-->|Raft RPCs| N3
    Prometheus -->|GET /metrics| N1
    Prometheus -->|GET /metrics| N2
    Prometheus -->|GET /metrics| N3
    Grafana --> Prometheus
~~~

The client arrow points to whichever node is currently leader. Followers reject
authoritative client operations with HTTP 409 and a known leader identifier;
they do not proxy requests.

## Node components

~~~mermaid
flowchart LR
    HTTP[FastAPI runtime]
    ClientAPI[Client API]
    AdminAPI[Health, status, metrics]
    PeerAPI[Raft peer API]
    Node[RaftNode]
    Election[Election tracker and timer]
    Replication[Leader replication state]
    Log[Logical Raft log]
    Store[KV state machine]
    Persistence[RaftPersistence]
    Transport[HTTP or in-memory transport]

    HTTP --> ClientAPI
    HTTP --> AdminAPI
    HTTP --> PeerAPI
    ClientAPI --> Node
    PeerAPI --> Node
    Node --> Election
    Node --> Replication
    Node --> Log
    Node --> Store
    Node --> Persistence
    Node --> Transport
~~~

Key responsibilities:

- FastAPI exposes client, peer, health, admin, and metrics routes.
- RaftNode owns consensus transitions and coordinates durable changes.
- LeaderReplication tracks next_index and match_index per follower.
- RaftLog encapsulates logical-to-physical index translation after compaction.
- KVStore is the deterministic state machine for PUT and DELETE commands.
- RaftPersistence stores term/vote/commit metadata, journal records, and
  snapshots.
- HTTPTransport is used between Docker nodes; InMemoryTransport provides the
  deterministic fault model used by tests.

## Write flow

~~~mermaid
sequenceDiagram
    participant C as Client
    participant L as Leader
    participant P as Leader persistence
    participant F as Followers
    participant S as KV state machine

    C->>L: PUT or DELETE
    L->>P: fsync appended log record
    par independent follower replication
        L->>F: AppendEntries
        F->>F: validate previous index/term
        F->>F: persist log/state
        F-->>L: success
    end
    L->>L: advance commit index after majority
    L->>P: persist commit index
    L->>S: apply committed commands in order
    L->>F: heartbeat with updated leader_commit
    L-->>C: committed response
~~~

Network calls are never made while the main Raft state lock is held. The leader
captures request state under the lock, releases it for transport I/O, then
reacquires it and verifies role and term before applying a response.

Replication uses persistent per-follower work so one slow peer does not delay a
healthy peer. Only one replication RPC per follower is in flight. A round waits
for enough useful responses within a bounded window rather than waiting for
every peer.

## Linearizable read flow

~~~mermaid
sequenceDiagram
    participant C as Client
    participant L as Leader
    participant F as Followers
    participant S as KV state machine

    C->>L: GET key
    alt valid 500 ms read lease in current term
        L->>S: ensure committed entries are applied
    else lease absent or expired
        par current-term heartbeat confirmation
            L->>F: AppendEntries heartbeat
            F-->>L: response
        end
        L->>L: require majority and unchanged role/term
        L->>S: apply committed entries
    end
    L-->>C: current value or not found
~~~

A follower never serves an authoritative GET. An isolated former leader can
use only an unexpired lease; after that it must confirm a quorum and fails the
read if it cannot. Higher-term responses force step-down and invalidate the
read.

## Election and leadership transfer

ElectionTimer chooses a random timeout between 1.5 and 3 seconds. The runtime
ticks approximately every 50 ms. A candidate persists its new term and
self-vote before collecting RequestVote responses concurrently.

For planned shutdown, a leader stops accepting new writes, catches up a healthy
follower, sends TimeoutNow, and steps down only after acceptance. This is a
conservative best-effort transfer, not a full membership-management protocol.

## Persistence model

A persistent node directory contains three files:

| File | Contents | Update pattern |
|---|---|---|
| raft-state.json | current_term, voted_for, commit_index | atomic replacement |
| raft-log.json | append, truncate, or journal snapshot records | fsynced append; atomic rewrite for compaction/migration |
| raft-snapshot.json | version, included index/term, KV state | atomic replacement |

Normal command appends add journal records and do not rewrite the entire log.
The loader remains compatible with the previous JSON-array log format and
migrates it to the journal representation.

A restarted node:

1. loads durable term, vote, and commit metadata;
2. loads the snapshot, or an empty index-zero snapshot if absent;
3. loads and validates the retained log suffix;
4. repairs the crash-safe case where the snapshot is ahead of the log base;
5. restores KV state from the snapshot;
6. applies committed retained entries after the snapshot boundary;
7. rejoins as a follower with no restored runtime leadership.

## Snapshots and logical indexes

The snapshot represents all commands through last_included_index at
last_included_term. RaftLog stores that boundary as base_index and base_term.
The first retained entry is base_index + 1.

~~~text
snapshot covers       retained journal suffix
[1 ... base_index]    [base_index+1 ... last_index]
~~~

term_at(base_index) returns base_term. Methods such as get, entries_from,
truncate_from, and append translate logical indexes inside RaftLog rather than
exposing list offsets to callers.

Compaction order is deliberately crash-safe:

1. choose no later than min(commit_index, last_applied);
2. capture state and boundary term;
3. atomically persist raft-snapshot.json;
4. compact and durably rewrite the retained journal;
5. update in-memory log and leader replication references.

A follower whose next_index is at or before the compacted boundary receives an
InstallSnapshot RPC. After durable installation, normal AppendEntries resumes
at last_included_index + 1.

## Concurrency boundaries

- An RLock protects role, term, vote, commit/apply indexes, log, snapshot, and
  leader replication state.
- A separate replication lock serializes replication-round bookkeeping.
- A persistent thread pool performs per-follower outbound work.
- Stale RPC responses are ignored after role or term changes.
- Persistence serializes file updates with its own write lock.
- KVStore protects its map independently.

These boundaries prevent network latency from blocking inbound Raft handlers
while keeping consensus mutations ordered.

## Failure and recovery behavior

The deterministic transport can block nodes or directional links, partition
groups, drop selected RPCs, and gate RPCs behind events. Integration tests
exercise follower loss, leader isolation, three-of-five majority partitions,
higher-term responses, divergent log repair, restart, snapshot retry, and
concurrent traffic.

The Docker demonstration validates the same operational path over real HTTP:
dynamic leader discovery, leader stop, new-term election, quorum operation,
restart, catch-up, and metrics.

## Deployment topology

docker-compose.yml defines three persistent node services, Prometheus, and
Grafana. Node ports 8001–8003 map to port 8000 in the Compose network.
Prometheus scrapes node1:8000, node2:8000, and node3:8000 every two seconds.
Grafana provisions the Prometheus datasource and dashboard from files committed
under monitoring/.

## Deliberate boundaries

The current architecture has static membership, one Raft group, whole-state
JSON snapshot transfer, and no transport security. It does not provide
sharding, dynamic reconfiguration, streamed snapshots, authentication, backup
or restore orchestration, or automated compaction scheduling.
