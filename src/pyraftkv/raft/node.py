

from concurrent.futures import (
    FIRST_COMPLETED,
    Future,
    ThreadPoolExecutor,
    as_completed,
    wait,
)
from pathlib import Path
from threading import Lock, RLock
from time import monotonic, perf_counter

from pyraftkv.raft.election import ElectionTracker
from pyraftkv.raft.election_trigger import start_election_if_needed
from pyraftkv.raft.leader import LeaderReplication
from pyraftkv.raft.log import LogEntry, RaftCommand, RaftLog
from pyraftkv.raft.persistence import RaftPersistence
from pyraftkv.raft.replication import handle_append_entries
from pyraftkv.raft.rpc import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    InstallSnapshotRequest,
    InstallSnapshotResponse,
    RequestVoteRequest,
    RequestVoteResponse,
    TimeoutNowRequest,
    TimeoutNowResponse,
)
from pyraftkv.raft.snapshot import RaftSnapshot
from pyraftkv.raft.state import NodeRole, RaftState
from pyraftkv.raft.state_machine import apply_committed_entries
from pyraftkv.raft.status import (
    FollowerStatus,
    RaftNodeStatus,
)
from pyraftkv.raft.timer import ElectionTimer
from pyraftkv.raft.vote import handle_request_vote
from pyraftkv.storage.store import KVStore
from pyraftkv.transport.base import RaftTransport, TransportError


class NotLeaderError(RuntimeError):
    """Raised when a leader-only client operation reaches a non-leader."""


class LeadershipTransferInProgressError(RuntimeError):
    """Raised when a write arrives during leadership transfer."""


class ReadQuorumError(RuntimeError):
    """Raised when leadership cannot be confirmed for a linearizable read."""


class RaftNode:
    def __init__(
        self,
        node_id: str,
        members: set[str],
        data_dir: str | Path | None = None,
    ) -> None:
        if node_id not in members:
            raise ValueError("node_id must be a cluster member")

        self.node_id = node_id
        self.members = set(members)
        self._raft_lock = RLock()
        self._replication_lock = Lock()
        self._replication_executor = ThreadPoolExecutor(
            max_workers=max(1, len(self.members) - 1),
            thread_name_prefix=f"raft-repl-{node_id}",
        )

        self._replication_futures: dict[
                str,
                Future[bool],
        ] = {}
        self.persistence = (
            RaftPersistence(data_dir)
            if data_dir is not None
            else None
        )

        # Load persisted Raft state, snapshot, and log.
        # Existing installations have an empty snapshot boundary.
        if self.persistence is None:
            self.state = RaftState(
                node_id=node_id,
            )
            self.snapshot = RaftSnapshot()
            self.log = RaftLog()
        else:
            self.state = self.persistence.load_state(
                node_id
            )
            self.snapshot = (
                self.persistence.load_snapshot()
            )
            self.log = self.persistence.load_log()

            if (
                self.snapshot.last_included_index
                < self.log.base_index
            ):
                raise RuntimeError(
                    "Raft log is compacted beyond durable snapshot"
                )

            if (
                self.snapshot.last_included_index
                == self.log.base_index
                and self.snapshot.last_included_term
                != self.log.base_term
            ):
                raise RuntimeError(
                    "Raft snapshot conflicts with log boundary"
                )

            if (
                self.snapshot.last_included_index
                > self.log.base_index
            ):
                # Crash-safe recovery when the snapshot was
                # persisted before log compaction completed.
                self.log.install_snapshot_boundary(
                    self.snapshot.last_included_index,
                    self.snapshot.last_included_term,
                )
                self.persistence.save_log(self.log)

        if (
            self.state.commit_index
            < self.snapshot.last_included_index
        ):
            self.state.commit_index = (
                self.snapshot.last_included_index
            )
            self._persist_state()

        # A persisted commit index must never point beyond
        # the snapshot boundary plus retained log suffix.
        if self.state.commit_index > self.log.last_index:
            raise RuntimeError(
                "Persisted commit index exceeds Raft log"
            )

        # Rebuild runtime KV state from the durable snapshot,
        # then replay committed entries in the retained suffix.
        self.store = KVStore()
        self.store.restore(self.snapshot.state)

        self.state.last_applied = (
            self.snapshot.last_included_index
        )

        if (
            self.state.commit_index
            > self.state.last_applied
        ):
            apply_committed_entries(
                self.state,
                self.log,
                self.store,
            )

        # Runtime leadership state is not restored.
        # A restarted node always rejoins as a follower.
        self.state.role = NodeRole.FOLLOWER
        self.state.leader_id = None

        # Runtime-only components are recreated on startup.
        self.timer = ElectionTimer()

        self.election = ElectionTracker(
            self.state,
            self.members,
        )

        self.replication: LeaderReplication | None = None
        self._read_lease_duration = 0.5
        self._last_read_quorum_term: int | None = None
        self._last_read_quorum_time: float | None = None
        self._leadership_transfer_in_progress = False

    def close(self, wait_for_workers: bool = False) -> None:
        """Release persistent follower-replication workers."""
        # Synchronize with an active inbound handler, but release
        # the Raft lock before workers are joined.
        with self._raft_lock:
            pass

        self._replication_executor.shutdown(
            wait=wait_for_workers,
            cancel_futures=True,
        )

    def status(self) -> RaftNodeStatus:
        """Return a detached, read-only snapshot of Raft state."""
        with self._raft_lock:
            followers: tuple[
                FollowerStatus,
                ...,
            ] | None = None

            if (
                self.state.role == NodeRole.LEADER
                and self.replication is not None
            ):
                followers = tuple(
                    FollowerStatus(
                        node_id=follower_id,
                        match_index=(
                            self.replication.match_index[
                                follower_id
                            ]
                        ),
                        next_index=(
                            self.replication.next_index[
                                follower_id
                            ]
                        ),
                        replication_lag=max(
                            0,
                            self.log.last_index
                            - self.replication.match_index[
                                follower_id
                            ],
                        ),
                    )
                    for follower_id in sorted(
                        self.replication.followers
                    )
                )

            return RaftNodeStatus(
                node_id=self.node_id,
                role=self.state.role.value,
                current_term=self.state.current_term,
                leader_id=self.state.leader_id,
                commit_index=self.state.commit_index,
                last_applied=self.state.last_applied,
                log_base_index=self.log.base_index,
                log_base_term=self.log.base_term,
                log_last_index=self.log.last_index,
                log_last_term=self.log.last_term,
                snapshot_index=(
                    self.snapshot.last_included_index
                ),
                snapshot_term=(
                    self.snapshot.last_included_term
                ),
                peers=tuple(
                    sorted(
                        self.members - {self.node_id}
                    )
                ),
                followers=followers,
            )

    def _persist_state(self) -> None:
        if self.persistence is None:
            return

        self.persistence.save_state(
            self.state
        )

    def _persist_log_change(
        self,
        before_entries: tuple[LogEntry, ...],
        after_entries: tuple[LogEntry, ...],
    ) -> None:
        """Persist only the part of the Raft log that changed."""
        if self.persistence is None:
            return

        if before_entries == after_entries:
            return

        common_prefix_length = 0

        while (
            common_prefix_length < len(before_entries)
            and common_prefix_length < len(after_entries)
            and before_entries[common_prefix_length]
            == after_entries[common_prefix_length]
        ):
            common_prefix_length += 1

        new_suffix = list(
            after_entries[
                common_prefix_length:
            ]
        )

        # Normal follower catch-up:
        #
        # before: [1, 2]
        # after:  [1, 2, 3, 4]
        #
        # Only append entries 3 and 4.
        if common_prefix_length == len(
            before_entries
        ):
            self.persistence.append_log_entries(
                new_suffix
            )
            return

        # Conflict repair or suffix truncation:
        #
        # before: [1, 2(old), 3(old)]
        # after:  [1, 2(new), 3(new)]
        #
        # Persist:
        # truncate from 2
        # append new 2
        # append new 3
        replace_from = before_entries[
            common_prefix_length
        ].index
        self.persistence.replace_log_suffix(
            replace_from,
            new_suffix,
        )
    def create_snapshot(self) -> bool:
        """Persist applied state, then compact the covered log prefix."""
        with self._raft_lock:
            snapshot_index = min(
                self.state.commit_index,
                self.state.last_applied,
            )

            if snapshot_index <= self.log.base_index:
                return False

            snapshot_term = self.log.term_at(
                snapshot_index
            )

            if snapshot_term is None:
                raise RuntimeError(
                    "Snapshot boundary is missing from Raft log"
                )

            snapshot = RaftSnapshot(
                last_included_index=snapshot_index,
                last_included_term=snapshot_term,
                state=self.store.snapshot(),
            )

            # The durable snapshot must exist before any log
            # entries it covers are removed.
            if self.persistence is not None:
                self.persistence.save_snapshot(snapshot)

            compacted_log = self.log.copy()
            compacted_log.compact_through(
                snapshot_index
            )

            if self.persistence is not None:
                self.persistence.save_log(
                    compacted_log
                )
                self._persist_state()

            self.snapshot = snapshot
            self.log = compacted_log

            if self.replication is not None:
                self.replication.log = self.log

            return True

    def compact_log(self) -> bool:
        return self.create_snapshot()


    def handle_request_vote(
        self,
        request: RequestVoteRequest,
    ) -> RequestVoteResponse:
        with self._raft_lock:
            response = handle_request_vote(
                self.state,
                request,
                local_last_index=self.log.last_index,
                local_last_term=self.log.last_term,
            )

            # Persist current_term and voted_for before
            # responding to the candidate.
            self._persist_state()

            if response.vote_granted:
                self.timer.reset()

            if self.state.role != NodeRole.LEADER:
                self.replication = None

            return response

    def handle_append_entries(
        self,
        request: AppendEntriesRequest,
    ) -> AppendEntriesResponse:
        with self._raft_lock:
            before_entries = tuple(
                self.log.entries_from(self.log.first_index)
            )

            before_state = (
                self.state.current_term,
                self.state.voted_for,
                self.state.commit_index,
            )

            response = handle_append_entries(
                state=self.state,
                log=self.log,
                request=request,
                store=self.store,
            )

            after_entries = tuple(
                self.log.entries_from(self.log.first_index)
            )

            after_state = (
                self.state.current_term,
                self.state.voted_for,
                self.state.commit_index,
            )

            # Persist only the changed portion of the log.
            # Empty heartbeats produce no log persistence.
            self._persist_log_change(
                before_entries,
                after_entries,
            )

            if after_state != before_state:
                self._persist_state()

            if response.success:
                self.timer.reset()

            if self.state.role != NodeRole.LEADER:
                self.replication = None

            return response
    def handle_install_snapshot(
        self,
        request: InstallSnapshotRequest,
    ) -> InstallSnapshotResponse:
        with self._raft_lock:
            if request.term < self.state.current_term:
                return InstallSnapshotResponse(
                    term=self.state.current_term,
                    success=False,
                )

            previous_term = self.state.current_term
            self.state.become_follower(
                term=request.term,
                leader_id=request.leader_id,
            )
            self.replication = None

            if (
                request.last_included_index < 0
                or request.last_included_term < 0
            ):
                if self.state.current_term != previous_term:
                    self._persist_state()
                return InstallSnapshotResponse(
                    term=self.state.current_term,
                    success=False,
                )

            if (
                request.last_included_index
                < self.snapshot.last_included_index
            ):
                if self.state.current_term != previous_term:
                    self._persist_state()
                self.timer.reset()
                return InstallSnapshotResponse(
                    term=self.state.current_term,
                    success=True,
                )

            local_boundary_term = self.log.term_at(
                request.last_included_index
            )

            if (
                self.state.commit_index
                > request.last_included_index
                and local_boundary_term
                != request.last_included_term
            ):
                if self.state.current_term != previous_term:
                    self._persist_state()
                return InstallSnapshotResponse(
                    term=self.state.current_term,
                    success=False,
                )

            new_snapshot = RaftSnapshot(
                last_included_index=(
                    request.last_included_index
                ),
                last_included_term=(
                    request.last_included_term
                ),
                state=request.state.copy(),
            )
            new_log = self.log.copy()

            try:
                new_log.install_snapshot_boundary(
                    request.last_included_index,
                    request.last_included_term,
                )
            except ValueError:
                if self.state.current_term != previous_term:
                    self._persist_state()
                return InstallSnapshotResponse(
                    term=self.state.current_term,
                    success=False,
                )

            new_commit_index = max(
                self.state.commit_index,
                request.last_included_index,
            )

            if new_commit_index > new_log.last_index:
                if self.state.current_term != previous_term:
                    self._persist_state()
                return InstallSnapshotResponse(
                    term=self.state.current_term,
                    success=False,
                )

            # Persist the state-machine snapshot before the
            # compacted journal, then persist commit metadata.
            if self.persistence is not None:
                self.persistence.save_snapshot(
                    new_snapshot
                )
                self.persistence.save_log(new_log)

            self.snapshot = new_snapshot
            self.log = new_log
            self.store.restore(new_snapshot.state)
            self.state.commit_index = new_commit_index
            self.state.last_applied = (
                request.last_included_index
            )

            if (
                self.state.commit_index
                > self.state.last_applied
            ):
                apply_committed_entries(
                    self.state,
                    self.log,
                    self.store,
                )

            self._persist_state()
            self.timer.reset()

            return InstallSnapshotResponse(
                term=self.state.current_term,
                success=True,
            )


    def handle_timeout_now(
        self,
        request: TimeoutNowRequest,
    ) -> TimeoutNowResponse:
        with self._raft_lock:
            if request.term < self.state.current_term:
                return TimeoutNowResponse(
                    term=self.state.current_term,
                    accepted=False,
                )

            previous_term = self.state.current_term

            if request.term > self.state.current_term:
                self.state.become_follower(
                    term=request.term,
                    leader_id=request.leader_id,
                )

            accepted = (
                self.state.role == NodeRole.FOLLOWER
                and self.state.leader_id == request.leader_id
            )

            if self.state.current_term != previous_term:
                self._persist_state()

            if accepted:
                self.timer.expire_now()

            return TimeoutNowResponse(
                term=self.state.current_term,
                accepted=accepted,
            )


    def tick(
        self,
        transport: RaftTransport,
    ) -> NodeRole:
        # Protect Raft state changes with the lock, but never
        # perform network I/O while holding it.
        with self._raft_lock:
            previous_term = self.state.current_term
            previous_vote = self.state.voted_for

            requests = start_election_if_needed(
                self.state,
                self.timer,
                self.election,
                local_last_index=self.log.last_index,
                local_last_term=self.log.last_term,
            )

            if (
                self.state.current_term != previous_term
                or self.state.voted_for != previous_vote
            ):
                self._persist_state()

            # Single-node cluster may elect itself immediately.
            if self.state.role == NodeRole.LEADER:
                self._initialize_leader_replication()
                return self.state.role

            current_role = self.state.role

        if not requests:
            return current_role

        executor = ThreadPoolExecutor(
            max_workers=len(requests),
        )

        futures = {
            executor.submit(
                transport.request_vote,
                peer_id,
                request,
            ): (peer_id, request)
            for peer_id, request in requests.items()
        }

        try:
            for future in as_completed(futures):
                peer_id, request = futures[future]

                try:
                    response = future.result()
                except TransportError:
                    with self._raft_lock:
                        if (
                            self.state.role
                            != NodeRole.CANDIDATE
                            or self.state.current_term
                            != request.term
                        ):
                            return self.state.role

                    continue

                with self._raft_lock:
                    # State may have changed while the RPC
                    # was in flight.
                    if (
                        self.state.role
                        != NodeRole.CANDIDATE
                        or self.state.current_term
                        != request.term
                    ):
                        return self.state.role

                    before_term = (
                        self.state.current_term
                    )

                    before_vote = (
                        self.state.voted_for
                    )

                    self.election.record_vote(
                        peer_id,
                        response,
                    )

                    if (
                        self.state.current_term
                        != before_term
                        or self.state.voted_for
                        != before_vote
                    ):
                        self._persist_state()

                    if (
                        self.state.role
                        == NodeRole.LEADER
                    ):
                        self._initialize_leader_replication()

                        return self.state.role

                    if (
                        self.state.role
                        == NodeRole.FOLLOWER
                    ):
                        self.replication = None

                        return self.state.role

            with self._raft_lock:
                return self.state.role

        finally:
            # Do not wait for a dead peer once quorum has
            # already been reached.
            executor.shutdown(
                wait=False,
                cancel_futures=True,
            )
    def _initialize_leader_replication(
        self,
    ) -> None:
        if self.state.role != NodeRole.LEADER:
            self.replication = None
            return

        if self.replication is None:
            self.replication = LeaderReplication(
                self.state,
                self.log,
                self.members,
            )

    def _replicate_to_follower(
        self,
        follower_id: str,
        transport: RaftTransport,
    ) -> bool:
        while True:
            # Build either request while protecting Raft state.
            # Network I/O happens after releasing the lock.
            with self._raft_lock:
                if self.state.role != NodeRole.LEADER:
                    return False

                if self.replication is None:
                    return False

                next_index = self.replication.next_index[
                    follower_id
                ]

                if next_index <= self.log.base_index:
                    if (
                        self.snapshot.last_included_index
                        != self.log.base_index
                        or self.snapshot.last_included_term
                        != self.log.base_term
                    ):
                        raise RuntimeError(
                            "Compacted log has no matching snapshot"
                        )

                    snapshot_request = InstallSnapshotRequest(
                        term=self.state.current_term,
                        leader_id=self.node_id,
                        last_included_index=(
                            self.snapshot.last_included_index
                        ),
                        last_included_term=(
                            self.snapshot.last_included_term
                        ),
                        state=self.snapshot.state.copy(),
                    )
                    append_request = None
                    request_term = snapshot_request.term
                else:
                    append_request = (
                        self.replication.build_request(
                            follower_id,
                            leader_commit=(
                                self.state.commit_index
                            ),
                        )
                    )
                    snapshot_request = None
                    request_term = append_request.term

            try:
                if snapshot_request is not None:
                    response = transport.install_snapshot(
                        follower_id,
                        snapshot_request,
                    )
                else:
                    assert append_request is not None
                    response = transport.append_entries(
                        follower_id,
                        append_request,
                    )
            except TransportError:
                return False

            with self._raft_lock:
                # The node may have changed term or stepped down
                # while the RPC was in flight.
                if (
                    self.state.role != NodeRole.LEADER
                    or self.state.current_term != request_term
                    or self.replication is None
                ):
                    return False

                if response.term > self.state.current_term:
                    self.state.become_follower(
                        term=response.term,
                    )
                    self._persist_state()
                    self.replication = None
                    return False

                if snapshot_request is not None:
                    if not response.success:
                        return False

                    self.replication.record_snapshot_success(
                        follower_id,
                        snapshot_request.last_included_index,
                    )

                    if (
                        self.replication.next_index[follower_id]
                        <= self.log.last_index
                    ):
                        continue

                    return True

                assert append_request is not None

                if response.success:
                    self.replication.record_success(
                        follower_id,
                        append_request,
                    )

                    if (
                        self.replication.next_index[follower_id]
                        <= self.log.last_index
                        or append_request.leader_commit
                        < self.state.commit_index
                    ):
                        continue

                    return True

                # The follower's log does not match.
                # Backtrack next_index and retry only this follower.
                self.replication.record_failure(
                    follower_id,
                )
    def _replicate_followers_concurrently(
        self,
        transport: RaftTransport,
        required_follower: str | None = None,
    ) -> dict[str, bool]:
        with self._raft_lock:
            if self.state.role != NodeRole.LEADER:
                return {}

            self._initialize_leader_replication()

            if self.replication is None:
                return {}

            followers = tuple(
                self.replication.followers
            )
            follower_quorum = len(self.members) // 2

        if not followers:
            return {}

        results: dict[str, bool] = {}
        active: dict[str, Future[bool]] = {}

        for follower_id in followers:
            future = self._replication_futures.get(
                follower_id
            )

            # Collect a completed request from an earlier round.
            if future is not None and future.done():
                results[follower_id] = future.result()

                del self._replication_futures[
                    follower_id
                ]

                future = None

            # Never queue another RPC while this follower
            # already has an outstanding request.
            if future is None:
                future = self._replication_executor.submit(
                    self._replicate_to_follower,
                    follower_id,
                    transport,
                )

                self._replication_futures[
                    follower_id
                ] = future

            active[follower_id] = future

        if not active:
            return results

        # Return after the first successful follower response.
        # Fast failures must not hide a healthy quorum response,
        # and a delayed peer must not extend the total wait budget.
        pending = set(active.values())
        deadline = perf_counter() + 0.25

        while pending:
            remaining = deadline - perf_counter()

            if remaining <= 0:
                break

            done, pending = wait(
                pending,
                timeout=remaining,
                return_when=FIRST_COMPLETED,
            )

            if not done:
                break

            for follower_id, future in active.items():
                if future not in done:
                    continue

                result = future.result()
                results[follower_id] = result

                if (
                    self._replication_futures.get(
                        follower_id
                    )
                    is future
                ):
                    del self._replication_futures[
                        follower_id
                    ]

            if (
                required_follower is None
                and sum(results.values())
                >= follower_quorum
            ):
                break

            if required_follower in results:
                break

        return results

    def replicate_log(
        self,
        transport: RaftTransport,
    ) -> None:
        # Only one replication round should modify next_index /
        # match_index at a time. This lock does NOT block inbound
        # Raft RPC handlers.
        with self._replication_lock:
            with self._raft_lock:
                if self.state.role != NodeRole.LEADER:
                    raise NotLeaderError(
                        "Only the leader can replicate log entries"
                    )

                self._initialize_leader_replication()

                if self.replication is None:
                    raise RuntimeError(
                        "Leader replication state is unavailable"
                    )

            self._replicate_followers_concurrently(
                transport
            )

            with self._raft_lock:
                if (
                    self.state.role != NodeRole.LEADER
                    or self.replication is None
                ):
                    return

                previous_commit_index = (
                    self.state.commit_index
                )

                self.replication.advance_commit_index()

                if (
                    self.state.commit_index
                    != previous_commit_index
                ):
                    self._persist_state()

                    apply_committed_entries(
                        self.state,
                        self.log,
                        self.store,
                    )
    def send_heartbeats(
        self,
        transport: RaftTransport,
    ) -> dict[str, bool]:
        with self._replication_lock:
            return self._replicate_followers_concurrently(
                transport
            )

    def confirm_read_quorum(
        self,
        transport: RaftTransport,
        timeout: float = 0.25,
    ) -> None:
        """Confirm leadership with a fresh current-term quorum."""
        with self._replication_lock:
            with self._raft_lock:
                if self.state.role != NodeRole.LEADER:
                    raise NotLeaderError(
                        f"Node {self.node_id} is not the leader"
                    )

                read_term = self.state.current_term
                self._initialize_leader_replication()

                if self.replication is None:
                    raise RuntimeError(
                        "Leader replication state is unavailable"
                    )

                followers = tuple(
                    self.replication.followers
                )
                quorum_size = len(self.members) // 2 + 1

            if quorum_size == 1:
                with self._raft_lock:
                    if (
                        self.state.role != NodeRole.LEADER
                        or self.state.current_term != read_term
                    ):
                        raise ReadQuorumError(
                            "Leadership changed during read confirmation"
                        )

                    self._last_read_quorum_term = read_term
                    self._last_read_quorum_time = monotonic()
                    apply_committed_entries(
                        self.state,
                        self.log,
                        self.store,
                    )
                return

            deadline = monotonic() + timeout
            fresh_futures: dict[str, Future[bool]] = {}
            confirmed: set[str] = set()
            failed: set[str] = set()

            while (
                1 + len(confirmed) < quorum_size
                and monotonic() < deadline
            ):
                active: set[Future[bool]] = set()

                for follower_id in followers:
                    if (
                        follower_id in confirmed
                        or follower_id in failed
                    ):
                        continue

                    future = self._replication_futures.get(
                        follower_id
                    )

                    if future is not None and future.done():
                        result = future.result()

                        if (
                            self._replication_futures.get(
                                follower_id
                            )
                            is future
                        ):
                            del self._replication_futures[
                                follower_id
                            ]

                        if (
                            fresh_futures.get(follower_id)
                            is future
                        ):
                            if result:
                                confirmed.add(follower_id)
                            else:
                                failed.add(follower_id)
                            continue

                        future = None

                    if future is None:
                        future = self._replication_executor.submit(
                            self._replicate_to_follower,
                            follower_id,
                            transport,
                        )
                        self._replication_futures[
                            follower_id
                        ] = future
                        fresh_futures[follower_id] = future

                    active.add(future)

                if 1 + len(confirmed) >= quorum_size:
                    break

                if not active:
                    break

                remaining = deadline - monotonic()
                if remaining <= 0:
                    break

                wait(
                    active,
                    timeout=remaining,
                    return_when=FIRST_COMPLETED,
                )

            with self._raft_lock:
                if (
                    self.state.role != NodeRole.LEADER
                    or self.state.current_term != read_term
                ):
                    raise ReadQuorumError(
                        "Leadership changed during read confirmation"
                    )

                if 1 + len(confirmed) < quorum_size:
                    raise ReadQuorumError(
                        "A current-term read quorum is unavailable"
                    )

                self._last_read_quorum_term = read_term
                self._last_read_quorum_time = monotonic()
                apply_committed_entries(
                    self.state,
                    self.log,
                    self.store,
                )

    def _has_valid_read_lease(self) -> bool:
        if self.state.role != NodeRole.LEADER:
            return False

        if (
            self._last_read_quorum_term
            != self.state.current_term
            or self._last_read_quorum_time is None
        ):
            return False

        elapsed = monotonic() - self._last_read_quorum_time

        return 0 <= elapsed < self._read_lease_duration

    def linearizable_get(
        self,
        key: str,
        transport: RaftTransport,
    ) -> str | None:
        with self._raft_lock:
            if self.state.role != NodeRole.LEADER:
                raise NotLeaderError(
                    f"Node {self.node_id} is not the leader"
                )

            if self._has_valid_read_lease():
                apply_committed_entries(
                    self.state,
                    self.log,
                    self.store,
                )
                return self.store.get(key)

        self.confirm_read_quorum(transport)

        with self._raft_lock:
            if not self._has_valid_read_lease():
                raise ReadQuorumError(
                    "Leadership changed after read confirmation"
                )

            return self.store.get(key)

    def transfer_leadership(
        self,
        transport: RaftTransport,
        target_id: str | None = None,
    ) -> bool:
        """Transfer leadership to a responding, fully caught-up follower."""
        with self._replication_lock:
            with self._raft_lock:
                if self.state.role != NodeRole.LEADER:
                    raise NotLeaderError(
                        f"Node {self.node_id} is not the leader"
                    )

                self._initialize_leader_replication()

                if self.replication is None:
                    return False

                if (
                    target_id is not None
                    and target_id not in self.replication.followers
                ):
                    raise ValueError(
                        f"Unknown transfer target: {target_id}"
                    )

                transfer_term = self.state.current_term
                self._leadership_transfer_in_progress = True

            try:
                results = self._replicate_followers_concurrently(
                    transport,
                    required_follower=target_id,
                )

                with self._raft_lock:
                    if (
                        self.state.role != NodeRole.LEADER
                        or self.state.current_term != transfer_term
                        or self.replication is None
                    ):
                        return False

                    eligible = [
                        follower_id
                        for follower_id in self.replication.followers
                        if (
                            results.get(follower_id) is True
                            and self.replication.match_index[
                                follower_id
                            ]
                            >= self.log.last_index
                        )
                    ]

                    if target_id is None:
                        if not eligible:
                            return False

                        selected = min(eligible)
                    elif target_id in eligible:
                        selected = target_id
                    else:
                        return False

                    request = TimeoutNowRequest(
                        term=transfer_term,
                        leader_id=self.node_id,
                    )

                try:
                    response = transport.timeout_now(
                        selected,
                        request,
                    )
                except TransportError:
                    return False

                with self._raft_lock:
                    if (
                        self.state.role != NodeRole.LEADER
                        or self.state.current_term != transfer_term
                    ):
                        return False

                    if response.term > self.state.current_term:
                        self.state.become_follower(
                            term=response.term,
                        )
                        self._persist_state()
                        self.replication = None
                        return False

                    if not response.accepted:
                        return False

                    self.state.become_follower(
                        term=transfer_term,
                    )
                    self.replication = None
                    return True
            finally:
                with self._raft_lock:
                    self._leadership_transfer_in_progress = False

    def submit_command(
        self,
        command: RaftCommand,
        transport: RaftTransport,
    ) -> bool:
        return self.submit_commands(
            [command],
            transport,
        )[0]
    def submit_commands(
        self,
        commands: list[RaftCommand],
        transport: RaftTransport,
    ) -> list[bool]:
        with self._raft_lock:
            if not commands:
                return []

            if self._leadership_transfer_in_progress:
                raise LeadershipTransferInProgressError(
                    "Leader is transferring leadership"
                )

            if self.state.role != NodeRole.LEADER:
                raise NotLeaderError(
                    f"Node {self.node_id} is not the leader"
                )

            for command in commands:
                if command.operation not in {
                    "PUT",
                    "DELETE",
                }:
                    raise ValueError(
                        f"Unsupported command: "
                        f"{command.operation}"
                    )

                if (
                    command.operation == "PUT"
                    and command.value is None
                ):
                    raise ValueError(
                        "PUT command requires a value"
                    )

            entries = [
                self.log.append(
                    term=self.state.current_term,
                    command=command,
                )
                for command in commands
            ]

            # Persist the new leader entries before replication.
            if self.persistence is not None:
                self.persistence.append_log_entries(
                    entries
                )

            self._initialize_leader_replication()

        # Network I/O must happen outside _raft_lock.
        self.replicate_log(
            transport
        )

        with self._raft_lock:
            if (
                self.state.role != NodeRole.LEADER
                or self.replication is None
            ):
                return [False] * len(entries)

            results = [
                self.state.commit_index >= entry.index
                for entry in entries
            ]

        if any(results):
            self.send_heartbeats(
                transport
            )

        return results
    def put(
        self,
        key: str,
        value: str,
        transport: RaftTransport,
    ) -> bool:
        return self.submit_command(
            RaftCommand(
                operation="PUT",
                key=key,
                value=value,
            ),
            transport,
        )

    def delete(
        self,
        key: str,
        transport: RaftTransport,
    ) -> bool:
        return self.submit_command(
            RaftCommand(
                operation="DELETE",
                key=key,
            ),
            transport,
        )