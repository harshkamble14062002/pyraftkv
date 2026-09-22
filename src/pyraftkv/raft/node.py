from pathlib import Path
from threading import RLock

from pyraftkv.raft.election import ElectionTracker
from pyraftkv.raft.election_trigger import start_election_if_needed
from pyraftkv.raft.leader import LeaderReplication
from pyraftkv.raft.log import RaftCommand, RaftLog
from pyraftkv.raft.persistence import RaftPersistence
from pyraftkv.raft.replication import handle_append_entries
from pyraftkv.raft.rpc import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    RequestVoteRequest,
    RequestVoteResponse,
)
from pyraftkv.raft.state import NodeRole, RaftState
from pyraftkv.raft.state_machine import apply_committed_entries
from pyraftkv.raft.timer import ElectionTimer
from pyraftkv.raft.vote import handle_request_vote
from pyraftkv.storage.store import KVStore
from pyraftkv.transport.base import RaftTransport, TransportError


class NotLeaderError(RuntimeError):
    """Raised when a client write is sent to a non-leader node."""


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
        self.persistence = RaftPersistence(data_dir) if data_dir is not None else None

        # Load persisted Raft state and log when persistence
        # is enabled. Otherwise start with fresh state.
        if self.persistence is None:
            self.state = RaftState(
                node_id=node_id,
            )
            self.log = RaftLog()
        else:
            self.state = self.persistence.load_state(node_id)
            self.log = self.persistence.load_log()

        # A persisted commit index must never point beyond
        # the available Raft log.
        if self.state.commit_index > self.log.last_index:
            raise RuntimeError("Persisted commit index exceeds Raft log")

        # KVStore is runtime state. Rebuild it by replaying
        # the committed portion of the Raft log.
        self.store = KVStore()

        self.state.last_applied = 0

        if self.state.commit_index > 0:
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

    def _persist_state(self) -> None:
        if self.persistence is None:
            return

        self.persistence.save_state(self.state)

    def _persist_log(self) -> None:
        if self.persistence is None:
            return

        self.persistence.save_log(self.log)

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
            before_entries = tuple(self.log.entries_from(1))

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

            after_entries = tuple(self.log.entries_from(1))

            after_state = (
                self.state.current_term,
                self.state.voted_for,
                self.state.commit_index,
            )

            # Do not rewrite the log for empty heartbeats.
            if after_entries != before_entries:
                self._persist_log()

            if after_state != before_state:
                self._persist_state()

            if response.success:
                self.timer.reset()

            if self.state.role != NodeRole.LEADER:
                self.replication = None

            return response

    def tick(
        self,
        transport: RaftTransport,
    ) -> NodeRole:
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

            # Starting an election changes current_term and
            # normally voted_for as well. Persist before sending RPCs.
            if (
                self.state.current_term != previous_term
                or self.state.voted_for != previous_vote
            ):
                self._persist_state()

            # Important for a single-node cluster.
            if self.state.role == NodeRole.LEADER:
                self._initialize_leader_replication()
                return self.state.role

            for peer_id in sorted(requests):
                request = requests[peer_id]

                try:
                    response = transport.request_vote(
                        peer_id,
                        request,
                    )
                except TransportError:
                    continue

                before_term = self.state.current_term
                before_vote = self.state.voted_for

                self.election.record_vote(
                    peer_id,
                    response,
                )

                # A higher-term response can force the
                # candidate back to follower.
                if (
                    self.state.current_term != before_term
                    or self.state.voted_for != before_vote
                ):
                    self._persist_state()

                if self.state.role == NodeRole.LEADER:
                    self._initialize_leader_replication()
                    break

                if self.state.role == NodeRole.FOLLOWER:
                    self.replication = None
                    break

            return self.state.role

    def _initialize_leader_replication(self) -> None:
        if self.state.role != NodeRole.LEADER:
            self.replication = None
            return

        if self.replication is None:
            self.replication = LeaderReplication(
                self.state,
                self.log,
                self.members,
            )

    def send_heartbeats(
        self,
        transport: RaftTransport,
    ) -> dict[str, bool]:
        with self._raft_lock:
            if self.state.role != NodeRole.LEADER:
                return {}

            self._initialize_leader_replication()

            if self.replication is None:
                return {}

            results: dict[str, bool] = {}

            for follower_id in sorted(self.replication.followers):
                next_index = self.replication.next_index[follower_id]

                prev_log_index = next_index - 1
                prev_log_term = self.log.term_at(prev_log_index)

                if prev_log_term is None:
                    raise RuntimeError("Invalid replication index")

                request = AppendEntriesRequest(
                    term=self.state.current_term,
                    leader_id=self.node_id,
                    prev_log_index=prev_log_index,
                    prev_log_term=prev_log_term,
                    entries=(),
                    leader_commit=self.state.commit_index,
                )

                try:
                    response = transport.append_entries(
                        follower_id,
                        request,
                    )
                except TransportError:
                    results[follower_id] = False
                    continue

                if response.term > self.state.current_term:
                    self.state.become_follower(
                        term=response.term,
                    )

                    # current_term changed and must be durable.
                    self._persist_state()

                    self.replication = None
                    results[follower_id] = False
                    break

                if response.success:
                    self.replication.record_success(
                        follower_id,
                        request,
                    )
                    results[follower_id] = True
                else:
                    self.replication.record_failure(
                        follower_id,
                    )
                    results[follower_id] = False

            return results

    def _replicate_to_follower(
        self,
        follower_id: str,
        transport: RaftTransport,
    ) -> bool:
        if self.replication is None:
            raise RuntimeError("Leader replication state is unavailable")

        while self.state.role == NodeRole.LEADER:
            request = self.replication.build_request(
                follower_id,
                leader_commit=self.state.commit_index,
            )

            try:
                response = transport.append_entries(
                    follower_id,
                    request,
                )
            except TransportError:
                return False

            if response.term > self.state.current_term:
                self.state.become_follower(
                    term=response.term,
                )

                # Persist the higher term before returning.
                self._persist_state()

                self.replication = None
                return False

            if response.success:
                self.replication.record_success(
                    follower_id,
                    request,
                )
                return True

            # Backtrack next_index and retry until the
            # follower finds a matching prefix.
            self.replication.record_failure(follower_id)

        return False

    def replicate_log(
        self,
        transport: RaftTransport,
    ) -> None:
        with self._raft_lock:
            if self.state.role != NodeRole.LEADER:
                raise NotLeaderError("Only the leader can replicate log entries")

            self._initialize_leader_replication()

            if self.replication is None:
                raise RuntimeError("Leader replication state is unavailable")

            for follower_id in sorted(self.replication.followers):
                self._replicate_to_follower(
                    follower_id,
                    transport,
                )

                if self.state.role != NodeRole.LEADER:
                    return

            previous_commit_index = self.state.commit_index

            self.replication.advance_commit_index()

            if self.state.commit_index != previous_commit_index:
                # commit_index is durable Raft state.
                self._persist_state()

                apply_committed_entries(
                    self.state,
                    self.log,
                    self.store,
                )

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

            if self.state.role != NodeRole.LEADER:
                raise NotLeaderError(f"Node {self.node_id} is not the leader")

            for command in commands:
                if command.operation not in {
                    "PUT",
                    "DELETE",
                }:
                    raise ValueError(f"Unsupported command: {command.operation}")

                if command.operation == "PUT" and command.value is None:
                    raise ValueError("PUT command requires a value")

            entries = [
                self.log.append(
                    term=self.state.current_term,
                    command=command,
                )
                for command in commands
            ]

            # Persist the complete batch before replication.
            self._persist_log()

            self._initialize_leader_replication()

            self.replicate_log(transport)

            if self.state.role != NodeRole.LEADER:
                return [False] * len(entries)

            if self.replication is None:
                return [False] * len(entries)

            results = [self.state.commit_index >= entry.index for entry in entries]

            if any(results):
                self.send_heartbeats(transport)

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
