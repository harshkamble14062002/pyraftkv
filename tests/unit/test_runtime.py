import argparse
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

from pyraftkv.raft.state import NodeRole
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

def test_metrics_endpoint():
    runtime = create_runtime(
        "node-1",
        {
            "node-2": "http://127.0.0.1:8002",
            "node-3": "http://127.0.0.1:8003",
        },
    )

    app = create_node_app(runtime)

    with TestClient(app) as client:
        response = client.get(
            "/metrics"
        )

    assert response.status_code == 200

    assert (
        "pyraftkv_raft_current_term"
        in response.text
    )

    assert (
        "pyraftkv_raft_commit_index"
        in response.text
    )

    assert (
        "pyraftkv_http_requests_total"
        in response.text

    )

def test_http_metrics_use_bounded_route_templates(
    tmp_path,
):
    runtime = create_runtime(
        "node-1",
        {
            "node-2": "http://127.0.0.1:8002",
            "node-3": "http://127.0.0.1:8003",
        },
        data_dir=tmp_path,
    )
    app = create_node_app(runtime)

    with TestClient(app) as client:
        response = client.get(
            "/kv/user-supplied-key"
        )

    assert response.status_code == 409
    assert runtime.metrics is not None

    output = runtime.metrics.render().decode()

    assert 'path="/kv/{key}"' in output
    assert "user-supplied-key" not in output


def test_runtime_transfers_leadership_on_shutdown(
    tmp_path,
):
    runtime = create_runtime(
        "node-1",
        {
            "node-2": "http://127.0.0.1:8002",
            "node-3": "http://127.0.0.1:8003",
        },
        data_dir=tmp_path,
    )
    runtime.node.state.current_term = 1
    runtime.node.state.role = NodeRole.LEADER
    runtime.node.state.leader_id = "node-1"
    runtime.node.replicate_log = Mock()
    runtime.node.transfer_leadership = Mock(
        return_value=True
    )

    app = create_node_app(runtime)

    with TestClient(app):
        pass

    runtime.node.transfer_leadership.assert_called_once_with(
        runtime.transport
    )