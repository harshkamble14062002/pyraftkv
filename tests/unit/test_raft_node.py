from unittest.mock import Mock

import pytest

from pyraftkv.raft.node import RaftNode
from pyraftkv.raft.rpc import (
    AppendEntriesRequest,
    RequestVoteRequest,
)
from pyraftkv.raft.state import NodeRole
from pyraftkv.transport.memory import InMemoryTransport

MEMBERS = {
    "node-1",
    "node-2",
    "node-3",
}


def test_new_node_starts_as_follower():
    node = RaftNode(
        "node-1",
        MEMBERS,
    )

    assert node.state.role == NodeRole.FOLLOWER
    assert node.state.current_term == 0

    assert node.log.last_index == 0


def test_node_must_be_cluster_member():
    with pytest.raises(ValueError):
        RaftNode(
            "fake-node",
            MEMBERS,
        )


def test_node_handles_request_vote():
    node = RaftNode(
        "node-2",
        MEMBERS,
    )

    node.timer = Mock()

    request = RequestVoteRequest(
        term=1,
        candidate_id="node-1",
        last_log_index=0,
        last_log_term=0,
    )

    response = node.handle_request_vote(request)

    assert response.vote_granted is True
    assert node.state.voted_for == "node-1"
    assert node.state.current_term == 1

    node.timer.reset.assert_called_once()


def test_node_handles_heartbeat():
    node = RaftNode(
        "node-2",
        MEMBERS,
    )

    node.timer = Mock()

    request = AppendEntriesRequest(
        term=1,
        leader_id="node-1",
        leader_commit=0,
    )

    response = node.handle_append_entries(request)

    assert response.success is True

    assert node.state.role == NodeRole.FOLLOWER
    assert node.state.leader_id == "node-1"

    node.timer.reset.assert_called_once()


def test_old_leader_is_rejected():
    node = RaftNode(
        "node-2",
        MEMBERS,
    )

    node.state.current_term = 5
    node.timer = Mock()

    request = AppendEntriesRequest(
        term=4,
        leader_id="node-1",
    )

    response = node.handle_append_entries(request)

    assert response.success is False
    assert node.state.current_term == 5

    node.timer.reset.assert_not_called()


def test_two_nodes_exchange_request_vote():
    transport = InMemoryTransport()

    node1 = RaftNode(
        "node-1",
        MEMBERS,
    )

    node2 = RaftNode(
        "node-2",
        MEMBERS,
    )

    transport.register(
        "node-1",
        node1,
    )

    transport.register(
        "node-2",
        node2,
    )

    request = RequestVoteRequest(
        term=1,
        candidate_id="node-1",
        last_log_index=0,
        last_log_term=0,
    )

    response = transport.request_vote(
        "node-2",
        request,
    )

    assert response.vote_granted is True

    assert node2.state.voted_for == "node-1"

def test_leader_write_uses_append_only_log_persistence(
    tmp_path,
):
    node = RaftNode(
        "node-1",
        {"node-1"},
        data_dir=tmp_path,
    )

    node.state.role = NodeRole.LEADER
    node.state.current_term = 1

    real_persistence = node.persistence

    assert real_persistence is not None

    node.persistence = Mock(
        wraps=real_persistence
    )

    transport = InMemoryTransport()

    committed = node.put(
        "name",
        "harsha",
        transport,
    )

    assert committed is True

    node.persistence.append_log_entries.assert_called_once()
    node.persistence.save_log.assert_not_called()

    persisted_entries = (
        node.persistence.append_log_entries.call_args.args[0]
    )

    assert len(persisted_entries) == 1

    entry = persisted_entries[0]

    assert entry.index == 1
    assert entry.term == 1
    assert entry.command.operation == "PUT"
    assert entry.command.key == "name"
    assert entry.command.value == "harsha"