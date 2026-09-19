from unittest.mock import Mock

import pytest

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

def test_follower_rejects_client_put():
    transport, nodes = create_cluster()

    follower = nodes["node-2"]

    with pytest.raises(NotLeaderError):
        follower.put(
            "x",
            "10",
            transport,
        )


def test_put_commits_with_one_unreachable_follower():
    transport, nodes = create_cluster()

    leader = elect_node1(
        transport,
        nodes,
    )

    transport.block("node-3")

    committed = leader.put(
        "name",
        "harsha",
        transport,
    )

    assert committed is True

    assert leader.store.get("name") == "harsha"
    assert nodes["node-2"].store.get("name") == "harsha"


def test_put_does_not_commit_without_quorum():
    transport, nodes = create_cluster()

    leader = elect_node1(
        transport,
        nodes,
    )

    transport.block("node-2")
    transport.block("node-3")

    committed = leader.put(
        "unsafe",
        "value",
        transport,
    )

    assert committed is False

    assert leader.state.commit_index == 0
    assert leader.store.get("unsafe") is None
