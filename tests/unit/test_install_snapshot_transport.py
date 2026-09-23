import json
from unittest.mock import Mock

import httpx
from fastapi.testclient import TestClient

from pyraftkv.api.raft_app import create_raft_app
from pyraftkv.raft.node import RaftNode
from pyraftkv.raft.rpc import (
    InstallSnapshotRequest,
    InstallSnapshotResponse,
)
from pyraftkv.transport.http import HTTPTransport
from pyraftkv.transport.memory import InMemoryTransport
from pyraftkv.transport.serialization import (
    install_snapshot_from_dict,
    install_snapshot_to_dict,
)


def make_request() -> InstallSnapshotRequest:
    return InstallSnapshotRequest(
        term=4,
        leader_id="node-1",
        last_included_index=10,
        last_included_term=3,
        state={"key": "value"},
    )


def test_install_snapshot_serialization_round_trip():
    request = make_request()

    assert install_snapshot_from_dict(
        install_snapshot_to_dict(request)
    ) == request


def test_in_memory_install_snapshot_is_forwarded():
    handler = Mock()
    handler.handle_install_snapshot.return_value = (
        InstallSnapshotResponse(
            term=4,
            success=True,
        )
    )
    transport = InMemoryTransport()
    transport.register("node-2", handler)
    request = make_request()

    response = transport.install_snapshot(
        "node-2",
        request,
    )

    assert response.success is True
    handler.handle_install_snapshot.assert_called_once_with(
        request
    )


def test_http_install_snapshot_posts_internal_rpc():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/raft/install-snapshot"
        assert json.loads(request.content) == {
            "term": 4,
            "leader_id": "node-1",
            "last_included_index": 10,
            "last_included_term": 3,
            "state": {"key": "value"},
        }
        return httpx.Response(
            200,
            json={
                "term": 4,
                "success": True,
            },
        )

    client = httpx.Client(
        transport=httpx.MockTransport(handler)
    )
    transport = HTTPTransport(
        {"node-2": "http://node-2:8000"},
        client=client,
    )

    try:
        response = transport.install_snapshot(
            "node-2",
            make_request(),
        )
    finally:
        client.close()

    assert response == InstallSnapshotResponse(
        term=4,
        success=True,
    )


def test_install_snapshot_http_endpoint(tmp_path):
    node = RaftNode(
        "node-2",
        {"node-1", "node-2", "node-3"},
        data_dir=tmp_path,
    )
    client = TestClient(create_raft_app(node))

    response = client.post(
        "/raft/install-snapshot",
        json=install_snapshot_to_dict(make_request()),
    )

    assert response.status_code == 200
    assert response.json() == {
        "term": 4,
        "success": True,
    }
    assert node.store.get("key") == "value"
    assert node.log.base_index == 10
