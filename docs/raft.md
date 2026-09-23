# Raft Behavior in PyRaftKV

## Scope

PyRaftKV implements a compact Raft consensus group for an educational
key-value store. Membership is fixed when each process starts. The replicated
commands are PUT and DELETE; GET uses a leader read protocol and is not written
to the log.

The implementation follows Raft's core term, vote, log-matching, majority
commit, and state-machine ordering rules. It also includes practical recovery
and liveness mechanisms needed by this repository.

## Roles and terms

Every process starts or restarts as a follower. Runtime leadership is never
restored from disk.

- A follower accepts valid leader contact and resets its election timer.
- A timed-out follower becomes a candidate, increments and persists its term,
  votes for itself, and requests votes concurrently.
- A candidate becomes leader after a majority grants votes.
- Any newer term observed in an RPC request or response causes a safe
  transition to follower and durable term persistence.

Election timeouts are randomized from 1.5 to 3 seconds. The background runtime
loop executes approximately every 50 ms.

## Voting

RequestVote includes the candidate term and its last log index and term. A node
grants at most one vote per term and rejects a candidate whose log is less
up-to-date. Term and vote changes are persisted before the response is returned.

An unavailable peer does not block a vote from a healthy peer because outbound
vote requests run concurrently.

## Replicated log

Each LogEntry has a logical index, term, and RaftCommand. The command contains
an operation, key, and optional value.

A leader maintains next_index and match_index for each follower. AppendEntries
contains the previous logical index and term plus a suffix of new entries. A
follower accepts the suffix only when its previous entry matches. Conflicting
suffixes are truncated and replaced.

Failed consistency checks decrement only that follower's next_index. Successful
responses advance its match_index and next_index.

## Commit and apply rules

A leader computes the quorum match position across itself and followers. It
advances commit_index only when:

- a majority has replicated the candidate index;
- the candidate is newer than the existing commit index; and
- the entry at that index belongs to the leader's current term.

Committed entries are applied to KVStore in increasing index order. The
invariant last_applied <= commit_index <= log.last_index is checked throughout
the deterministic failure suite.

A successful client write means the leader committed that entry. It does not
mean every follower had already applied it at the instant the response was
returned; later heartbeats propagate the commit index.

## Follower replication concurrency

The leader uses a persistent thread pool and at most one in-flight task per
follower. Network I/O occurs outside the main Raft lock.

A replication round waits only for the responses needed to make progress within
a bounded window. It does not join a dead peer's worker. A fast transport
failure also does not cause the round to return before a healthy quorum response
has a chance to arrive.

After each response, the leader reacquires its state lock and verifies that it
is still leader in the request term before mutating replication state. This
prevents stale responses from an earlier role or term from corrupting progress.

## Linearizable reads

Followers reject authoritative reads. A leader uses one of two paths:

1. If it recently confirmed a current-term quorum and the conservative 500 ms
   monotonic lease remains valid, it applies any committed entries and reads.
2. Otherwise it sends heartbeat-style AppendEntries requests and requires a
   fresh majority response in the same role and term.

The read barrier does not append a dummy log entry. Failure to reach a majority,
a role change, or a higher-term response fails the read. The lease uses only
local monotonic elapsed time and is shorter than the minimum election timeout;
it does not rely on synchronized clocks.

## Leadership transfer

During planned shutdown, a leader enters a transfer state that rejects new
writes. It tries to select a responding follower whose match_index reaches the
leader's last log index, sends TimeoutNow, and steps down after the follower
accepts.

If catch-up or TimeoutNow fails, the operation reports failure without
discarding committed data. This is a conservative best-effort mechanism, not
dynamic cluster reconfiguration.

## Snapshots and compaction

RaftSnapshot version 1 stores:

~~~json
{
  "version": 1,
  "last_included_index": 120,
  "last_included_term": 8,
  "state": {
    "key": "value"
  }
}
~~~

RaftLog records the same boundary as base_index and base_term. Retained entries
begin at base_index + 1, and term_at(base_index) returns base_term.

Only committed and applied state can be compacted. The snapshot file is
atomically persisted before the log prefix is removed. If a crash happens
between those steps, startup recognizes that the durable snapshot is ahead of
the journal boundary and safely compacts the loaded log to match.

When a leader sees next_index at or before its compacted boundary, it sends
InstallSnapshot instead of retrying unavailable entries. The follower persists
the new snapshot and log boundary before acknowledging success. The leader then
continues normal AppendEntries from the first retained index.

Snapshots are sent as a whole JSON state object. Chunking and automatic
compaction thresholds are not implemented.

## Durable files

raft-state.json is atomically replaced and contains current_term, voted_for,
and commit_index.

raft-log.json is a newline-delimited journal with append and truncate records.
Compaction or legacy migration writes a journal snapshot record containing the
base boundary and retained entries. The loader accepts the older JSON-array log
format and migrates it.

raft-snapshot.json is an atomically replaced versioned state-machine snapshot.
A missing file means the backward-compatible empty boundary at index and term
zero.

Normal appends flush and fsync before replication can treat them as durable.
Snapshot installation persists snapshot, log, and state before returning
success.

## Tested invariants

The deterministic suites check:

- at most one observed leader per term;
- commit indexes never decrease;
- no node applies beyond its commit index;
- no node commits beyond its logical last index;
- retained indexes remain contiguous;
- committed entries at the same index do not disagree;
- minority partitions cannot commit or confirm reads;
- majority partitions continue;
- higher-term responses force durable step-down;
- healed nodes converge, including after restart and snapshot installation.

## Deliberate omissions

The implementation does not currently include joint consensus, membership
changes, pre-vote, leader leases negotiated across clocks, streamed snapshots,
or multiple Raft groups. Those are outside the v1.0 scope.
