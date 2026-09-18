from unittest.mock import Mock

from pyraftkv.raft.election import ElectionTracker
from pyraftkv.raft.election_trigger import start_election_if_needed
from pyraftkv.raft.state import NodeRole, RaftState


def test_election_does_not_start_before_timeout():
    state = RaftState(node_id="node-1")

    election = ElectionTracker(
        state,
        {"node-1", "node-2", "node-3"},
    )

    timer = Mock()
    timer.expired.return_value = False

    requests = start_election_if_needed(
        state,
        timer,
        election,
        local_last_index=0,
        local_last_term=0,
    )

    assert requests == {}
    assert state.role == NodeRole.FOLLOWER

    timer.reset.assert_not_called()


def test_timeout_starts_election():
    state = RaftState(node_id="node-1")

    election = ElectionTracker(
        state,
        {"node-1", "node-2", "node-3"},
    )

    timer = Mock()
    timer.expired.return_value = True

    requests = start_election_if_needed(
        state,
        timer,
        election,
        local_last_index=5,
        local_last_term=2,
    )

    assert state.role == NodeRole.CANDIDATE
    assert state.current_term == 1
    assert state.voted_for == "node-1"

    assert set(requests) == {
        "node-2",
        "node-3",
    }

    timer.reset.assert_called_once()


def test_request_contains_log_metadata():
    state = RaftState(node_id="node-1")

    election = ElectionTracker(
        state,
        {"node-1", "node-2"},
    )

    timer = Mock()
    timer.expired.return_value = True

    requests = start_election_if_needed(
        state,
        timer,
        election,
        local_last_index=20,
        local_last_term=4,
    )

    request = requests["node-2"]

    assert request.term == 1
    assert request.candidate_id == "node-1"
    assert request.last_log_index == 20
    assert request.last_log_term == 4


def test_single_node_cluster_becomes_leader():
    state = RaftState(node_id="node-1")

    election = ElectionTracker(
        state,
        {"node-1"},
    )

    timer = Mock()
    timer.expired.return_value = True

    requests = start_election_if_needed(
        state,
        timer,
        election,
        local_last_index=0,
        local_last_term=0,
    )

    assert requests == {}
    assert state.role == NodeRole.LEADER
    assert state.leader_id == "node-1"
