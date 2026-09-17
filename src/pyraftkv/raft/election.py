from pyraftkv.raft.rpc import RequestVoteResponse
from pyraftkv.raft.state import NodeRole, RaftState


class ElectionTracker:
    def __init__(
        self,
        state: RaftState,
        members: set[str],
    ) -> None:
        if not members:
            raise ValueError("Cluster must contain at least one member")

        if state.node_id not in members:
            raise ValueError("Local node must be a cluster member")

        self.state = state
        self.members = set(members)
        self.votes_received: set[str] = set()
        self.active = False

    @property
    def quorum_size(self) -> int:
        return len(self.members) // 2 + 1

    def start(self) -> None:
        self.state.become_candidate()

        self.votes_received = {
            self.state.node_id,
        }

        self.active = True

        self._promote_if_quorum()

    def record_vote(
        self,
        voter_id: str,
        response: RequestVoteResponse,
    ) -> bool:
        if not self.active:
            return False

        # A higher term means this candidate is stale.
        if response.term > self.state.current_term:
            self.state.become_follower(response.term)
            self.active = False
            return False

        # Ignore stale responses from older terms.
        if response.term < self.state.current_term:
            return False

        if self.state.role != NodeRole.CANDIDATE:
            self.active = False
            return False

        # Votes from unknown nodes must never count toward quorum.
        if voter_id not in self.members:
            return False

        if response.vote_granted:
            self.votes_received.add(voter_id)

        return self._promote_if_quorum()

    def _promote_if_quorum(self) -> bool:
        if len(self.votes_received) < self.quorum_size:
            return False

        self.state.become_leader()
        self.active = False

        return True
