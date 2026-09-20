from unittest.mock import Mock

import pytest

from pyraftkv.raft.log import RaftCommand
from pyraftkv.raft.node import NotLeaderError, RaftNode
from pyraftkv.raft.state import NodeRole
from pyraftkv.transport.memory import InMemoryTransport

MEMBERS = {
    "node-1",
    "node-2",
    "node-3",
}


def create_cluster():
    transport = InMemoryTransport()

    nodes = {
        node_id: RaftNode(node_id, MEMBERS)
        for node_id in MEMBERS
    }

    for node_id, node in nodes.items():
        transport.register(node_id, node)

    return transport, nodes


def elect_node1(transport, nodes):
    leader = nodes["node-1"]

    leader.timer = Mock()
    leader.timer.expired.return_value = True

    leader.tick(transport)

    assert leader.state.role == NodeRole.LEADER

    return leader


def test_generic_put_command_is_committed():
    transport, nodes = create_cluster()
    leader = elect_node1(transport, nodes)

    committed = leader.submit_command(
        RaftCommand(
            operation="PUT",
            key="language",
            value="python",
        ),
        transport,
    )

    assert committed is True
    assert leader.store.get("language") == "python"
    assert nodes["node-2"].store.get("language") == "python"
    assert nodes["node-3"].store.get("language") == "python"


def test_delete_is_replicated_and_committed():
    transport, nodes = create_cluster()
    leader = elect_node1(transport, nodes)

    assert leader.put(
        "language",
        "python",
        transport,
    )

    committed = leader.delete(
        "language",
        transport,
    )

    assert committed is True
    assert leader.store.get("language") is None
    assert nodes["node-2"].store.get("language") is None
    assert nodes["node-3"].store.get("language") is None


def test_delete_is_recorded_in_raft_log():
    transport, nodes = create_cluster()
    leader = elect_node1(transport, nodes)

    assert leader.put(
        "x",
        "10",
        transport,
    )

    assert leader.delete(
        "x",
        transport,
    )

    entry = leader.log.get(2)

    assert entry is not None
    assert entry.command.operation == "DELETE"
    assert entry.command.key == "x"


def test_delete_commits_with_one_unreachable_follower():
    transport, nodes = create_cluster()
    leader = elect_node1(transport, nodes)

    assert leader.put(
        "name",
        "harsha",
        transport,
    )

    transport.block("node-3")

    committed = leader.delete(
        "name",
        transport,
    )

    assert committed is True
    assert leader.store.get("name") is None
    assert nodes["node-2"].store.get("name") is None


def test_delete_does_not_apply_without_quorum():
    transport, nodes = create_cluster()
    leader = elect_node1(transport, nodes)

    assert leader.put(
        "safe",
        "value",
        transport,
    )

    transport.block("node-2")
    transport.block("node-3")

    committed = leader.delete(
        "safe",
        transport,
    )

    assert committed is False
    assert leader.store.get("safe") == "value"


def test_follower_rejects_delete():
    transport, nodes = create_cluster()

    follower = nodes["node-2"]

    with pytest.raises(NotLeaderError):
        follower.delete(
            "x",
            transport,
        )


def test_invalid_command_is_rejected_before_append():
    transport, nodes = create_cluster()
    leader = elect_node1(transport, nodes)

    with pytest.raises(ValueError):
        leader.submit_command(
            RaftCommand(
                operation="DROP_DATABASE",
                key="x",
            ),
            transport,
        )

    assert leader.log.last_index == 0
