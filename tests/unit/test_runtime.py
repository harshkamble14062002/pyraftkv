import argparse

import pytest
from fastapi.testclient import TestClient

from pyraftkv.runtime import (
    create_node_app,
    create_runtime,
    parse_peer,
)


def test_parse_peer():
    node_id, address = parse_peer("node-2=http://127.0.0.1:8002")

    assert node_id == "node-2"
    assert address == "http://127.0.0.1:8002"


def test_invalid_peer_format():
    with pytest.raises(argparse.ArgumentTypeError):
        parse_peer("node-2")


def test_invalid_peer_protocol():
    with pytest.raises(argparse.ArgumentTypeError):
        parse_peer("node-2=127.0.0.1:8002")


def test_runtime_creates_cluster_membership():
    runtime = create_runtime(
        "node-1",
        {
            "node-2": "http://127.0.0.1:8002",
            "node-3": "http://127.0.0.1:8003",
        },
    )

    assert runtime.node.members == {
        "node-1",
        "node-2",
        "node-3",
    }

    runtime.transport.close()


def test_local_node_cannot_be_peer():
    with pytest.raises(ValueError):
        create_runtime(
            "node-1",
            {
                "node-1": "http://127.0.0.1:8001",
            },
        )


def test_health_endpoint():
    runtime = create_runtime(
        "node-1",
        {
            "node-2": "http://127.0.0.1:8002",
            "node-3": "http://127.0.0.1:8003",
        },
    )

    app = create_node_app(runtime)

    with TestClient(app) as client:
        response = client.get("/health")

        assert response.status_code == 200

        data = response.json()

        assert data["status"] == "ok"
        assert data["node_id"] == "node-1"
        assert data["role"] == "follower"
        assert data["term"] == 0
