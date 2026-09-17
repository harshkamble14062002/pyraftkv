from pyraftkv.raft.rpc import (
    AppendEntriesRequest,
    AppendEntriesResponse,
)
from pyraftkv.raft.state import RaftState
from pyraftkv.raft.timer import ElectionTimer


def handle_heartbeat(
    state: RaftState,
    timer: ElectionTimer,
    request: AppendEntriesRequest,
) -> AppendEntriesResponse:
    if request.term < state.current_term:
        return AppendEntriesResponse(
            term=state.current_term,
            success=False,
        )

    state.become_follower(
        term=request.term,
        leader_id=request.leader_id,
    )

    timer.reset()

    return AppendEntriesResponse(
        term=state.current_term,
        success=True,
    )
