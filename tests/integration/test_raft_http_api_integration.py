from fastapi.testclient import TestClient

from pyraftkv.api.raft_app import create_raft_app
from pyraftkv.raft.node import RaftNode

MEMBERS = {
    "node-1",
    "node-2",
    "node-3",
}


def create_client(
    node_id: str = "node-2",
):
    node = RaftNode(
        node_id,
        MEMBERS,
    )

    app = create_raft_app(node)

    client = TestClient(app)

    return client, node


def test_timeout_now_endpoint_expires_follower_timer():
    client, node = create_client()
    node.state.current_term = 2
    node.state.leader_id = "node-1"

    response = client.post(
        "/raft/timeout-now",
        json={
            "term": 2,
            "leader_id": "node-1",
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "term": 2,
        "accepted": True,
    }
    assert node.timer.expired() is True


def test_timeout_now_endpoint_rejects_invalid_payload():
    client, _ = create_client()

    response = client.post(
        "/raft/timeout-now",
        json={"term": "invalid"},
    )

    assert response.status_code == 400
