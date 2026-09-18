from pyraftkv.raft.election import ElectionTracker
from pyraftkv.raft.election_trigger import start_election_if_needed
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
                break

        return self.state.role
