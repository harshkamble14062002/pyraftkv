from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

from pyraftkv.raft.node import RaftNode
from pyraftkv.raft.state import NodeRole
from pyraftkv.transport.memory import InMemoryTransport

MEMBERS = {"node-1", "node-2", "node-3"}

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

def test_concurrent_puts_are_serialized():
    transport, nodes = create_cluster()
    leader = elect_node1(transport, nodes)
    def write(index: int) -> bool:
        return leader.put(f"key-{index}", f"value-{index}", transport)
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(write, range(50)))
    assert all(results)
    assert leader.state.commit_index == 50
    assert leader.state.last_applied == 50
    for index in range(50):
        expected = f"value-{index}"
        assert leader.store.get(f"key-{index}") == expected
        assert nodes["node-2"].store.get(f"key-{index}") == expected
        assert nodes["node-3"].store.get(f"key-{index}") == expected
