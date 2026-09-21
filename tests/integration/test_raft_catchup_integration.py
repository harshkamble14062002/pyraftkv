from unittest.mock import Mock

from pyraftkv.raft.log import LogEntry, RaftCommand
from pyraftkv.raft.node import RaftNode
from pyraftkv.raft.state import NodeRole
from pyraftkv.transport.memory import InMemoryTransport

MEMBERS = {
    "node-1",
    "node-2",
    "node-3",
}


def entry(
    index: int,
    term: int,
    key: str,
) -> LogEntry:
    return LogEntry(
        index=index,
        term=term,
        command=RaftCommand(
            operation="PUT",
            key=key,
            value=key,
        ),
    )


def create_cluster():
    transport = InMemoryTransport()

    nodes = {node_id: RaftNode(node_id, MEMBERS) for node_id in MEMBERS}

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


def test_lagging_follower_catches_up():
    transport, nodes = create_cluster()
    leader = elect_node1(transport, nodes)

    follower = nodes["node-3"]

    leader.log.append(
        term=leader.state.current_term,
        command=RaftCommand(
            operation="PUT",
            key="a",
            value="1",
        ),
    )

    leader.log.append(
        term=leader.state.current_term,
        command=RaftCommand(
            operation="PUT",
            key="b",
            value="2",
        ),
    )

    leader.log.append(
        term=leader.state.current_term,
        command=RaftCommand(
            operation="PUT",
            key="c",
            value="3",
        ),
    )

    # Follower only has entry 1.
    entry = leader.log.get(1)
    assert entry is not None
    follower.log.append_entry(entry)

    assert leader.replication is not None

    # Simulate an over-optimistic leader after leadership change.
    leader.replication.next_index["node-3"] = 4

    success = leader._replicate_to_follower(
        "node-3",
        transport,
    )

    assert success is True

    assert follower.log.last_index == 3

    assert follower.log.get(2) == leader.log.get(2)
    assert follower.log.get(3) == leader.log.get(3)

    assert leader.replication.next_index["node-3"] == 4


def test_divergent_follower_suffix_is_replaced():
    transport, nodes = create_cluster()
    leader = elect_node1(transport, nodes)

    follower = nodes["node-2"]

    leader.log.append(
        term=leader.state.current_term,
        command=RaftCommand(
            operation="PUT",
            key="x",
            value="leader-1",
        ),
    )

    leader.log.append(
        term=leader.state.current_term,
        command=RaftCommand(
            operation="PUT",
            key="x",
            value="leader-2",
        ),
    )

    first_entry = leader.log.get(1)

    assert first_entry is not None

    follower.log.append_entry(first_entry)

    # Same index but conflicting term.
    follower.log.append(
        term=99,
        command=RaftCommand(
            operation="PUT",
            key="x",
            value="wrong-value",
        ),
    )

    assert leader.replication is not None

    leader.replication.next_index["node-2"] = 3

    success = leader._replicate_to_follower(
        "node-2",
        transport,
    )

    assert success is True

    follower_entry = follower.log.get(2)
    leader_entry = leader.log.get(2)

    assert follower_entry == leader_entry


def test_unreachable_follower_stops_retrying():
    transport, nodes = create_cluster()
    leader = elect_node1(transport, nodes)

    transport.block("node-3")

    assert leader.replication is not None

    success = leader._replicate_to_follower(
        "node-3",
        transport,
    )

    assert success is False
    assert leader.state.role == NodeRole.LEADER
