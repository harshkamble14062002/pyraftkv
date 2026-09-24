from concurrent.futures import ThreadPoolExecutor
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


def vote_request(
    candidate_id: str,
) -> RequestVoteRequest:
    return RequestVoteRequest(
        term=1,
        candidate_id=candidate_id,
        last_log_index=0,
        last_log_term=0,
    )


def make_transport() -> tuple[
    InMemoryTransport,
    Mock,
    Mock,
]:
    transport = InMemoryTransport()
    node_1 = Mock()
    node_2 = Mock()
    node_1.handle_request_vote.return_value = (
        RequestVoteResponse(
            term=1,
            vote_granted=True,
        )
    )
    node_2.handle_request_vote.return_value = (
        RequestVoteResponse(
            term=1,
            vote_granted=True,
        )
    )
    node_2.handle_append_entries.return_value = (
        AppendEntriesResponse(
            term=1,
            success=True,
        )
    )
    transport.register("node-1", node_1)
    transport.register("node-2", node_2)

    return transport, node_1, node_2


def test_directional_link_only_blocks_selected_direction():
    transport, _, _ = make_transport()
    transport.block_link("node-1", "node-2")

    with pytest.raises(TransportError):
        transport.request_vote(
            "node-2",
            vote_request("node-1"),
        )

    response = transport.request_vote(
        "node-1",
        vote_request("node-2"),
    )

    assert response.vote_granted is True


def test_partition_blocks_both_directions_until_healed():
    transport, _, _ = make_transport()
    transport.partition({"node-1"}, {"node-2"})

    with pytest.raises(TransportError):
        transport.request_vote(
            "node-2",
            vote_request("node-1"),
        )

    with pytest.raises(TransportError):
        transport.request_vote(
            "node-1",
            vote_request("node-2"),
        )

    transport.heal_partition()

    assert transport.request_vote(
        "node-2",
        vote_request("node-1"),
    ).vote_granted


def test_drop_is_counted_and_consumed_once():
    transport, _, _ = make_transport()
    request = AppendEntriesRequest(
        term=1,
        leader_id="node-1",
    )
    transport.drop_append_entries(
        "node-1",
        "node-2",
    )

    with pytest.raises(TransportError):
        transport.append_entries(
            "node-2",
            request,
        )

    assert transport.append_entries(
        "node-2",
        request,
    ).success
    assert transport.call_count(
        "append_entries",
        "node-1",
        "node-2",
    ) == 2


def test_delay_is_event_gated_and_releasable():
    transport, _, _ = make_transport()
    request = vote_request("node-1")
    transport.delay_request_vote(
        "node-1",
        "node-2",
    )

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            transport.request_vote,
            "node-2",
            request,
        )

        assert not future.done()

        transport.release(
            "request_vote",
            "node-1",
            "node-2",
        )

        assert future.result(timeout=0.5).vote_granted


def test_blocked_node_cannot_send_or_receive():
    transport, _, _ = make_transport()
    transport.block("node-1")

    with pytest.raises(TransportError):
        transport.request_vote(
            "node-2",
            vote_request("node-1"),
        )

    with pytest.raises(TransportError):
        transport.request_vote(
            "node-1",
            vote_request("node-2"),
        )
