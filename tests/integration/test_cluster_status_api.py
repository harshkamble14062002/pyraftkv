from fastapi import FastAPI
from fastapi.testclient import TestClient

from pyraftkv.api.admin import create_admin_router
from pyraftkv.raft.log import RaftCommand
from pyraftkv.raft.node import RaftNode
from pyraftkv.raft.state import NodeRole

MEMBERS = {
    "node-1",
    "node-2",
    "node-3",
}


def create_client(node: RaftNode) -> TestClient:
    app = FastAPI()
    app.include_router(create_admin_router(node))
    return TestClient(app)


def test_cluster_status_reports_follower_state():
    node = RaftNode("node-2", MEMBERS)
    node.state.current_term = 3
    node.state.leader_id = "node-1"
    client = create_client(node)

    response = client.get("/cluster/status")

    assert response.status_code == 200
    assert response.json() == {
        "node_id": "node-2",
        "role": "follower",
        "current_term": 3,
        "leader_id": "node-1",
        "commit_index": 0,
        "last_applied": 0,
        "log_base_index": 0,
        "log_base_term": 0,
        "log_last_index": 0,
        "log_last_term": 0,
        "snapshot_index": 0,
        "snapshot_term": 0,
        "peers": ["node-1", "node-3"],
        "followers": None,
    }


def test_cluster_status_reports_detached_leader_progress():
    node = RaftNode("node-1", MEMBERS)
    node.state.role = NodeRole.LEADER
    node.state.current_term = 5
    node.state.leader_id = node.node_id
    node.log.append(
        term=5,
        command=RaftCommand(
            operation="PUT",
            key="language",
            value="python",
        ),
    )
    node._initialize_leader_replication()

    assert node.replication is not None

    client = create_client(node)
    payload = client.get("/cluster/status").json()

    assert payload["role"] == "leader"
    assert payload["log_last_index"] == 1
    assert payload["followers"] == [
        {
            "node_id": "node-2",
            "match_index": 0,
            "next_index": 2,
            "replication_lag": 1,
        },
        {
            "node_id": "node-3",
            "match_index": 0,
            "next_index": 2,
            "replication_lag": 1,
        },
    ]

    payload["followers"][0]["match_index"] = 99

    refreshed = client.get("/cluster/status").json()

    assert refreshed["followers"][0]["match_index"] == 0
    assert node.replication.match_index["node-2"] == 0
