from unittest.mock import Mock

from pyraftkv.raft.node import RaftNode
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
        node_id: RaftNode(
            node_id,
            MEMBERS,
        )
        for node_id in MEMBERS
    }

    for node_id, node in nodes.items():
        transport.register(
            node_id,
            node,
        )

    return transport, nodes


def elect(
    node: RaftNode,
    transport: InMemoryTransport,
) -> None:
    node.timer = Mock()
    node.timer.expired.return_value = True

    node.tick(transport)

    assert node.state.role == NodeRole.LEADER


def test_new_leader_accepts_write_after_failover():
    transport, nodes = create_cluster()

    node1 = nodes["node-1"]
    elect(node1, transport)

    assert node1.put(
        "before",
        "failure",
        transport,
    )

    transport.block("node-1")

    node2 = nodes["node-2"]
    elect(node2, transport)

    committed = node2.put(
        "after",
        "failover",
        transport,
    )

    assert committed is True

    assert node2.store.get("before") == "failure"
    assert node2.store.get("after") == "failover"

    assert nodes["node-3"].store.get("before") == "failure"
    assert nodes["node-3"].store.get("after") == "failover"


def test_restarted_node_catches_up_from_new_leader():
    transport, nodes = create_cluster()

    old_leader = nodes["node-1"]

    elect(
        old_leader,
        transport,
    )

    assert old_leader.put(
        "before",
        "failure",
        transport,
    )

    # Simulate node-1 crashing.
    transport.block("node-1")

    new_leader = nodes["node-2"]

    elect(
        new_leader,
        transport,
    )

    assert new_leader.put(
        "after",
        "failover",
        transport,
    )

    # Replace the dead process with a brand-new RaftNode.
    restarted = RaftNode(
        "node-1",
        MEMBERS,
    )

    transport.unregister("node-1")

    transport.register(
        "node-1",
        restarted,
    )

    transport.unblock("node-1")

    # Periodic leader replication should repair it.
    new_leader.replicate_log(transport)

    assert restarted.state.role == NodeRole.FOLLOWER

    assert restarted.state.current_term == new_leader.state.current_term

    assert restarted.log.last_index == new_leader.log.last_index

    assert restarted.store.get("before") == "failure"

    assert restarted.store.get("after") == "failover"
