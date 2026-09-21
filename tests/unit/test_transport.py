from unittest.mock import Mock

import pytest

from pyraftkv.raft.rpc import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    RequestVoteRequest,
    RequestVoteResponse,
)
from pyraftkv.transport.base import TransportError
from pyraftkv.transport.memory import InMemoryTransport


def test_request_vote_is_forwarded():
    handler = Mock()

    handler.handle_request_vote.return_value = RequestVoteResponse(
        term=2,
        vote_granted=True,
    )

    transport = InMemoryTransport()

    transport.register(
        "node-2",
        handler,
    )

    request = RequestVoteRequest(
        term=2,
        candidate_id="node-1",
        last_log_index=0,
        last_log_term=0,
    )

    response = transport.request_vote(
        "node-2",
        request,
    )

    assert response.vote_granted is True

    handler.handle_request_vote.assert_called_once_with(request)


def test_append_entries_is_forwarded():
    handler = Mock()

    handler.handle_append_entries.return_value = AppendEntriesResponse(
        term=2,
        success=True,
    )

    transport = InMemoryTransport()

    transport.register(
        "node-2",
        handler,
    )

    request = AppendEntriesRequest(
        term=2,
        leader_id="node-1",
    )

    response = transport.append_entries(
        "node-2",
        request,
    )

    assert response.success is True


def test_blocked_node_is_unreachable():
    handler = Mock()

    transport = InMemoryTransport()

    transport.register(
        "node-2",
        handler,
    )

    transport.block("node-2")

    request = RequestVoteRequest(
        term=1,
        candidate_id="node-1",
        last_log_index=0,
        last_log_term=0,
    )

    with pytest.raises(TransportError):
        transport.request_vote(
            "node-2",
            request,
        )


def test_node_can_be_unblocked():
    handler = Mock()

    handler.handle_request_vote.return_value = RequestVoteResponse(
        term=1,
        vote_granted=True,
    )

    transport = InMemoryTransport()

    transport.register(
        "node-2",
        handler,
    )

    transport.block("node-2")
    transport.unblock("node-2")

    request = RequestVoteRequest(
        term=1,
        candidate_id="node-1",
        last_log_index=0,
        last_log_term=0,
    )

    response = transport.request_vote(
        "node-2",
        request,
    )

    assert response.vote_granted is True
