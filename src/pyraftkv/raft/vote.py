from pyraftkv.raft.rpc import (
    RequestVoteRequest,
    RequestVoteResponse,
)
from pyraftkv.raft.state import RaftState


def is_candidate_log_up_to_date(
    candidate_last_index: int,
    candidate_last_term: int,
    local_last_index: int,
    local_last_term: int,
) -> bool:
    if candidate_last_term != local_last_term:
        return candidate_last_term > local_last_term

    return candidate_last_index >= local_last_index


def handle_request_vote(
    state: RaftState,
    request: RequestVoteRequest,
    *,
    local_last_index: int,
    local_last_term: int,
) -> RequestVoteResponse:
    # Candidate is from an older term.
    if request.term < state.current_term:
        return RequestVoteResponse(
            term=state.current_term,
            vote_granted=False,
        )

    # Any higher term means our current information is stale.
    if request.term > state.current_term:
        state.become_follower(term=request.term)

    can_vote = (
        state.voted_for is None
        or state.voted_for == request.candidate_id
    )

    log_is_current = is_candidate_log_up_to_date(
        candidate_last_index=request.last_log_index,
        candidate_last_term=request.last_log_term,
        local_last_index=local_last_index,
        local_last_term=local_last_term,
    )

    vote_granted = can_vote and log_is_current

    if vote_granted:
        state.voted_for = request.candidate_id

    return RequestVoteResponse(
        term=state.current_term,
        vote_granted=vote_granted,
    )
