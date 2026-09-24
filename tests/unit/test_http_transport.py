import json

import httpx

from pyraftkv.raft.rpc import TimeoutNowRequest
from pyraftkv.transport.http import HTTPTransport


def test_timeout_now_posts_internal_rpc():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/raft/timeout-now"
        assert json.loads(request.content) == {
            "term": 3,
            "leader_id": "node-1",
        }
        return httpx.Response(
            200,
            json={
                "term": 3,
                "accepted": True,
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
        response = transport.timeout_now(
            "node-2",
            TimeoutNowRequest(
                term=3,
                leader_id="node-1",
            ),
        )
    finally:
        client.close()

    assert response.term == 3
    assert response.accepted is True
