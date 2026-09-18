from pyraftkv.raft.commit import update_follower_commit
from pyraftkv.raft.log import RaftLog
from pyraftkv.raft.rpc import (
    AppendEntriesRequest,
    AppendEntriesResponse,
)
from pyraftkv.raft.state import RaftState
from pyraftkv.storage.store import KVStore


def handle_append_entries(
    state: RaftState,
    log: RaftLog,
    request: AppendEntriesRequest,
    store: KVStore | None = None,
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

    # The follower does not contain the previous log entry.
    if request.prev_log_index > log.last_index:
        return AppendEntriesResponse(
            term=state.current_term,
            success=False,
        )

    # The previous entry exists but has a different term.
    local_prev_term = log.term_at(request.prev_log_index)

    if local_prev_term != request.prev_log_term:
        return AppendEntriesResponse(
            term=state.current_term,
            success=False,
        )

    # Synchronize follower log with the leader.
    for entry in request.entries:
        existing = log.get(entry.index)

        if existing is not None:
            # Entry already matches the leader.
            if existing.term == entry.term:
                continue

            # Conflict: remove this entry and everything after it.
            log.truncate_from(entry.index)

        log.append_entry(entry)

    # Update follower commit index and apply newly committed entries
    # when a state machine is available.
    if store is not None:
        update_follower_commit(
            state,
            log,
            store,
            request.leader_commit,
        )

    return AppendEntriesResponse(
        term=state.current_term,
        success=True,
    )
