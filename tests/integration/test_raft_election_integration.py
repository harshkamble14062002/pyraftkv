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
        node_id: RaftNode(node_id, MEMBERS)
        for node_id in MEMBERS
    }

    for node_id, node in nodes.items():
        transport.register(node_id, node)

    return transport, nodes

def test_three_node_cluster_elects_leader():
    transport, nodes = create_cluster()

    node1 = nodes["node-1"]

    node1.timer = Mock()
    node1.timer.expired.return_value = True

    role = node1.tick(transport)

    assert role == NodeRole.LEADER
    assert node1.state.role == NodeRole.LEADER
    assert node1.state.leader_id == "node-1"


def test_election_succeeds_with_one_unreachable_follower():
    transport, nodes = create_cluster()

    transport.block("node-2")

    node1 = nodes["node-1"]

    node1.timer = Mock()
    node1.timer.expired.return_value = True

    role = node1.tick(transport)

    assert role == NodeRole.LEADER

def test_election_fails_without_quorum():
    transport, nodes = create_cluster()

    transport.block("node-2")
    transport.block("node-3")

    node1 = nodes["node-1"]

    node1.timer = Mock()
    node1.timer.expired.return_value = True

    role = node1.tick(transport)

    assert role == NodeRole.CANDIDATE
    assert node1.state.role == NodeRole.CANDIDATE

def test_no_election_before_timeout():
    transport, nodes = create_cluster()

    node1 = nodes["node-1"]

    node1.timer = Mock()
    node1.timer.expired.return_value = False

    role = node1.tick(transport)

    assert role == NodeRole.FOLLOWER
    assert node1.state.current_term == 0