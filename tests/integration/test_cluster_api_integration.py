from unittest.mock import Mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from pyraftkv.api.cluster import create_cluster_router
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


def elect_node1(transport, nodes):
    leader = nodes["node-1"]

    leader.timer = Mock()
    leader.timer.expired.return_value = True

    leader.tick(transport)

    assert leader.state.role == NodeRole.LEADER

    return leader


def create_client(node, transport):
    app = FastAPI()

    app.include_router(
        create_cluster_router(
            node,
            transport,
        )
    )

    return TestClient(app)


def test_put_through_leader_api():
    transport, nodes = create_cluster()

    leader = elect_node1(
        transport,
        nodes,
    )

    client = create_client(
        leader,
        transport,
    )

    response = client.put(
        "/kv/language",
        json={
            "value": "python",
        },
    )

    assert response.status_code == 200
    assert response.json()["committed"] is True

    assert leader.store.get("language") == "python"
    assert nodes["node-2"].store.get("language") == "python"
    assert nodes["node-3"].store.get("language") == "python"


def test_get_committed_value():
    transport, nodes = create_cluster()

    leader = elect_node1(
        transport,
        nodes,
    )

    leader.put(
        "language",
        "python",
        transport,
    )

    follower = nodes["node-2"]

    client = create_client(
        follower,
        transport,
    )

    response = client.get("/kv/language")

    assert response.status_code == 200
    assert response.json()["value"] == "python"


def test_follower_rejects_put():
    transport, nodes = create_cluster()

    leader = elect_node1(
        transport,
        nodes,
    )

    follower = nodes["node-2"]

    # Send a heartbeat so follower knows the current leader.
    leader.send_heartbeats(transport)

    client = create_client(
        follower,
        transport,
    )

    response = client.put(
        "/kv/x",
        json={
            "value": "10",
        },
    )

    assert response.status_code == 409

    detail = response.json()["detail"]

    assert detail["error"] == "not_leader"
    assert detail["leader_id"] == "node-1"


def test_delete_through_leader_api():
    transport, nodes = create_cluster()

    leader = elect_node1(
        transport,
        nodes,
    )

    leader.put(
        "name",
        "harsha",
        transport,
    )

    client = create_client(
        leader,
        transport,
    )

    response = client.delete("/kv/name")

    assert response.status_code == 200
    assert response.json()["committed"] is True

    assert leader.store.get("name") is None
    assert nodes["node-2"].store.get("name") is None
    assert nodes["node-3"].store.get("name") is None


def test_put_returns_503_without_quorum():
    transport, nodes = create_cluster()

    leader = elect_node1(
        transport,
        nodes,
    )

    transport.block("node-2")
    transport.block("node-3")

    client = create_client(
        leader,
        transport,
    )

    response = client.put(
        "/kv/unsafe",
        json={
            "value": "value",
        },
    )

    assert response.status_code == 503

    assert leader.store.get("unsafe") is None
