from unittest.mock import Mock

from pyraftkv.raft.heartbeat import handle_heartbeat
from pyraftkv.raft.rpc import AppendEntriesRequest
from pyraftkv.raft.state import NodeRole, RaftState


def test_valid_heartbeat_is_accepted():
    state = RaftState(
        node_id="node-2",
        current_term=3,
    )

    timer = Mock()

    response = handle_heartbeat(
        state,
        timer,
        AppendEntriesRequest(
            term=3,
            leader_id="node-1",
        ),
    )

    assert response.success is True
    assert response.term == 3
    assert state.role == NodeRole.FOLLOWER
    assert state.leader_id == "node-1"

    timer.reset.assert_called_once()


def test_old_term_heartbeat_is_rejected():
    state = RaftState(
        node_id="node-2",
        current_term=5,
    )

    timer = Mock()

    response = handle_heartbeat(
        state,
        timer,
        AppendEntriesRequest(
            term=4,
            leader_id="node-1",
        ),
    )

    assert response.success is False
    assert response.term == 5

    timer.reset.assert_not_called()


def test_candidate_steps_down_on_valid_heartbeat():
    state = RaftState(node_id="node-2")
    state.become_candidate()

    timer = Mock()

    handle_heartbeat(
        state,
        timer,
        AppendEntriesRequest(
            term=state.current_term,
            leader_id="node-1",
        ),
    )

    assert state.role == NodeRole.FOLLOWER
    assert state.leader_id == "node-1"


def test_higher_term_heartbeat_updates_term():
    state = RaftState(
        node_id="node-2",
        current_term=3,
        role=NodeRole.LEADER,
    )

    timer = Mock()

    response = handle_heartbeat(
        state,
        timer,
        AppendEntriesRequest(
            term=4,
            leader_id="node-1",
        ),
    )

    assert response.success is True
    assert state.current_term == 4
    assert state.role == NodeRole.FOLLOWER
    assert state.leader_id == "node-1"
