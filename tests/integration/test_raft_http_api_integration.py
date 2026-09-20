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
