from pyraftkv.raft.election import ElectionTracker
from pyraftkv.raft.rpc import RequestVoteRequest
from pyraftkv.raft.state import NodeRole, RaftState
from pyraftkv.raft.timer import ElectionTimer


def start_election_if_needed(
    state: RaftState,
    timer: ElectionTimer,
    election: ElectionTracker,
    *,
    local_last_index: int,
    local_last_term: int,
) -> dict[str, RequestVoteRequest]:
    if not timer.expired():
        return {}

    election.start()
    timer.reset()

    # Single-node clusters may already have elected themselves.
    if state.role == NodeRole.LEADER:
        return {}

    request = RequestVoteRequest(
        term=state.current_term,
        candidate_id=state.node_id,
        last_log_index=local_last_index,
        last_log_term=local_last_term,
    )

    return {
        peer_id: request for peer_id in election.members if peer_id != state.node_id
    }
