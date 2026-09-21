from unittest.mock import Mock

from pyraftkv.raft.node import RaftNode
from pyraftkv.raft.state import NodeRole
from pyraftkv.transport.memory import InMemoryTransport

MEMBERS = {
    "node-1",
    "node-2",
    "node-3",
}


def create_cluster(tmp_path):
    transport = InMemoryTransport()

    nodes = {
        node_id: RaftNode(
            node_id,
            MEMBERS,
            data_dir=tmp_path / node_id,
        )
        for node_id in MEMBERS
    }

    for node_id, node in nodes.items():
        transport.register(
            node_id,
            node,
        )

    return transport, nodes


def elect_node1(
    transport,
    nodes,
):
    leader = nodes["node-1"]

    leader.timer = Mock()
    leader.timer.expired.return_value = True

    leader.tick(transport)

    assert leader.state.role == NodeRole.LEADER

    return leader


def test_full_cluster_restart_preserves_committed_data(
    tmp_path,
):
    transport, nodes = create_cluster(tmp_path)

    leader = elect_node1(
        transport,
        nodes,
    )

    committed = leader.put(
        "language",
        "python",
        transport,
    )

    assert committed is True

    # Simulate complete process shutdown:
    # discard every in-memory node.
    new_transport = InMemoryTransport()

    restarted = {
        node_id: RaftNode(
            node_id,
            MEMBERS,
            data_dir=tmp_path / node_id,
        )
        for node_id in MEMBERS
    }

    for node_id, node in restarted.items():
        new_transport.register(
            node_id,
            node,
        )

    for node in restarted.values():
        assert node.state.role == NodeRole.FOLLOWER

        assert node.store.get("language") == "python"

        assert node.log.last_index == 1
        assert node.state.commit_index == 1


def test_uncommitted_entry_is_not_applied_after_restart(
    tmp_path,
):
    transport, nodes = create_cluster(tmp_path)

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

    restarted = RaftNode(
        "node-1",
        MEMBERS,
        data_dir=tmp_path / "node-1",
    )

    assert restarted.log.last_index == 1

    assert restarted.state.commit_index == 0

    assert restarted.store.get("unsafe") is None
