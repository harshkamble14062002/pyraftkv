from pyraftkv.raft.rpc import RequestVoteRequest
from pyraftkv.raft.state import NodeRole, RaftState
from pyraftkv.raft.vote import handle_request_vote


def make_request(
    *,
    term: int = 1,
    candidate_id: str = "node-2",
    last_log_index: int = 0,
    last_log_term: int = 0,
) -> RequestVoteRequest:
    return RequestVoteRequest(
        term=term,
        candidate_id=candidate_id,
        last_log_index=last_log_index,
        last_log_term=last_log_term,
    )


def test_rejects_older_term():
    state = RaftState(
        node_id="node-1",
        current_term=3,
    )

    response = handle_request_vote(
        state,
        make_request(term=2),
        local_last_index=0,
        local_last_term=0,
    )

    assert response.vote_granted is False
    assert response.term == 3


def test_higher_term_updates_node():
    state = RaftState(
        node_id="node-1",
        current_term=2,
        role=NodeRole.LEADER,
    )

    response = handle_request_vote(
        state,
        make_request(term=3),
        local_last_index=0,
        local_last_term=0,
    )

    assert response.vote_granted is True
    assert state.current_term == 3
    assert state.role == NodeRole.FOLLOWER
    assert state.voted_for == "node-2"


def test_node_does_not_vote_for_two_candidates():
    state = RaftState(
        node_id="node-1",
        current_term=3,
        voted_for="node-2",
    )

    response = handle_request_vote(
        state,
        make_request(
            term=3,
            candidate_id="node-3",
        ),
        local_last_index=0,
        local_last_term=0,
    )

    assert response.vote_granted is False
    assert state.voted_for == "node-2"


def test_same_candidate_can_receive_vote_again():
    state = RaftState(
        node_id="node-1",
        current_term=3,
        voted_for="node-2",
    )

    response = handle_request_vote(
        state,
        make_request(
            term=3,
            candidate_id="node-2",
        ),
        local_last_index=0,
        local_last_term=0,
    )

    assert response.vote_granted is True


def test_rejects_candidate_with_older_log_term():
    state = RaftState(
        node_id="node-1",
        current_term=3,
    )

    response = handle_request_vote(
        state,
        make_request(
            term=3,
            last_log_index=100,
            last_log_term=4,
        ),
        local_last_index=20,
        local_last_term=5,
    )

    assert response.vote_granted is False


def test_rejects_shorter_log_when_terms_match():
    state = RaftState(
        node_id="node-1",
        current_term=3,
    )

    response = handle_request_vote(
        state,
        make_request(
            term=3,
            last_log_index=19,
            last_log_term=5,
        ),
        local_last_index=20,
        local_last_term=5,
    )

    assert response.vote_granted is False


def test_accepts_newer_log_term():
    state = RaftState(
        node_id="node-1",
        current_term=3,
    )

    response = handle_request_vote(
        state,
        make_request(
            term=3,
            last_log_index=5,
            last_log_term=6,
        ),
        local_last_index=100,
        local_last_term=5,
    )

    assert response.vote_granted is True


def test_accepts_equal_log():
    state = RaftState(
        node_id="node-1",
        current_term=3,
    )

    response = handle_request_vote(
        state,
        make_request(
            term=3,
            last_log_index=20,
            last_log_term=5,
        ),
        local_last_index=20,
        local_last_term=5,
    )

    assert response.vote_granted is True
