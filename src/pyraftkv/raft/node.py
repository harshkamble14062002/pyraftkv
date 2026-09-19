from pyraftkv.raft.election import ElectionTracker
from pyraftkv.raft.election_trigger import start_election_if_needed
from pyraftkv.raft.leader import LeaderReplication
from pyraftkv.raft.log import RaftLog
from pyraftkv.raft.replication import handle_append_entries
from pyraftkv.raft.rpc import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    RequestVoteRequest,
    RequestVoteResponse,
)
from pyraftkv.raft.state import NodeRole, RaftState
from pyraftkv.raft.timer import ElectionTimer
from pyraftkv.raft.vote import handle_request_vote
from pyraftkv.storage.store import KVStore
from pyraftkv.transport.base import RaftTransport, TransportError


class RaftNode:
    def __init__(
        self,
        node_id: str,
        members: set[str],
    ) -> None:
        if node_id not in members:
            raise ValueError("node_id must be a cluster member")

        self.node_id = node_id
        self.members = set(members)

        self.state = RaftState(
            node_id=node_id,
        )

        self.log = RaftLog()
        self.store = KVStore()

        self.timer = ElectionTimer()

        self.election = ElectionTracker(
            self.state,
            self.members,
        )

        self.replication: LeaderReplication | None = None

    def handle_request_vote(
        self,
        request: RequestVoteRequest,
    ) -> RequestVoteResponse:
        response = handle_request_vote(
            self.state,
            request,
            local_last_index=self.log.last_index,
            local_last_term=self.log.last_term,
        )

        if response.vote_granted:
            self.timer.reset()

        if self.state.role != NodeRole.LEADER:
            self.replication = None

        return response

    def handle_append_entries(
        self,
        request: AppendEntriesRequest,
    ) -> AppendEntriesResponse:
        response = handle_append_entries(
            state=self.state,
            log=self.log,
            request=request,
            store=self.store,
        )

        if response.success:
            self.timer.reset()

        if self.state.role != NodeRole.LEADER:
            self.replication = None

        return response

    def tick(
        self,
        transport: RaftTransport,
    ) -> NodeRole:
        requests = start_election_if_needed(
            self.state,
            self.timer,
            self.election,
            local_last_index=self.log.last_index,
            local_last_term=self.log.last_term,
        )

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

            self.election.record_vote(
                peer_id,
                response,
            )

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
