from pyraftkv.raft.log import RaftLog
from pyraftkv.raft.rpc import (
    AppendEntriesRequest,
    AppendEntriesResponse,
)
from pyraftkv.raft.state import RaftState


def handle_append_entries(
    state: RaftState,
    log: RaftLog,
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

    # Follower does not contain the previous entry.
    if request.prev_log_index > log.last_index:
        return AppendEntriesResponse(
            term=state.current_term,
            success=False,
        )

    # Previous entry exists, but belongs to a different term.
    local_prev_term = log.term_at(request.prev_log_index)

    if local_prev_term != request.prev_log_term:
        return AppendEntriesResponse(
            term=state.current_term,
            success=False,
        )

    for entry in request.entries:
        existing = log.get(entry.index)

        if existing is not None:
            if existing.term == entry.term:
                continue

            # Conflict: delete this entry and everything after it.
            log.truncate_from(entry.index)

        log.append_entry(entry)

    return AppendEntriesResponse(
        term=state.current_term,
        success=True,
    )
