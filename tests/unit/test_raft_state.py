import pytest

from pyraftkv.raft.state import NodeRole, RaftState


def test_new_node_starts_as_follower():
    state = RaftState(node_id="node-1")

    assert state.role == NodeRole.FOLLOWER
    assert state.current_term == 0
    assert state.voted_for is None
    assert state.leader_id is None


def test_become_candidate_increments_term():
    state = RaftState(node_id="node-1")

    state.become_candidate()

    assert state.role == NodeRole.CANDIDATE
    assert state.current_term == 1
    assert state.voted_for == "node-1"
    assert state.leader_id is None


def test_candidate_can_become_leader():
    state = RaftState(node_id="node-1")

    state.become_candidate()
    state.become_leader()

    assert state.role == NodeRole.LEADER
    assert state.leader_id == "node-1"


def test_higher_term_resets_vote():
    state = RaftState(
        node_id="node-1",
        current_term=2,
        voted_for="node-2",
    )

    state.become_follower(term=3)

    assert state.current_term == 3
    assert state.voted_for is None
    assert state.role == NodeRole.FOLLOWER


def test_cannot_move_to_older_term():
    state = RaftState(
        node_id="node-1",
        current_term=5,
    )

    with pytest.raises(ValueError):
        state.become_follower(term=4)


def test_follower_cannot_directly_become_leader():
    state = RaftState(node_id="node-1")

    with pytest.raises(RuntimeError):
        state.become_leader()
