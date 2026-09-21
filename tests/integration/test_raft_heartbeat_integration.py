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

    nodes = {node_id: RaftNode(node_id, MEMBERS) for node_id in MEMBERS}

    for node_id, node in nodes.items():
        transport.register(node_id, node)

    return transport, nodes


def test_leader_sends_heartbeat_to_followers():
    transport, nodes = create_cluster()

    leader = nodes["node-1"]

    leader.timer = Mock()
    leader.timer.expired.return_value = True

    leader.tick(transport)

    assert leader.state.role == NodeRole.LEADER

    results = leader.send_heartbeats(transport)

    assert results == {
        "node-2": True,
        "node-3": True,
    }

    assert nodes["node-2"].state.leader_id == "node-1"
    assert nodes["node-3"].state.leader_id == "node-1"


def test_unreachable_follower_does_not_crash_leader():
    transport, nodes = create_cluster()

    leader = nodes["node-1"]

    leader.timer = Mock()
    leader.timer.expired.return_value = True

    leader.tick(transport)

    transport.block("node-3")

    results = leader.send_heartbeats(transport)

    assert results["node-2"] is True
    assert results["node-3"] is False

    assert leader.state.role == NodeRole.LEADER


def test_leader_steps_down_on_higher_term_response():
    transport, nodes = create_cluster()

    leader = nodes["node-1"]

    leader.timer = Mock()
    leader.timer.expired.return_value = True

    leader.tick(transport)

    assert leader.state.role == NodeRole.LEADER

    nodes["node-2"].state.current_term = leader.state.current_term + 1

    leader.send_heartbeats(transport)

    assert leader.state.role == NodeRole.FOLLOWER
    assert leader.replication is None
