from threading import Event, Thread

import pytest

from pyraftkv.raft.node import (
    LeadershipTransferInProgressError,
    NotLeaderError,
    RaftNode,
)
from pyraftkv.raft.rpc import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    TimeoutNowRequest,
    TimeoutNowResponse,
)
from pyraftkv.raft.state import NodeRole
from pyraftkv.transport.base import TransportError
from pyraftkv.transport.memory import InMemoryTransport

MEMBERS = {"node-1", "node-2", "node-3"}


def create_leader() -> tuple[
    RaftNode,
    dict[str, RaftNode],
    InMemoryTransport,
]:
    nodes = {
        node_id: RaftNode(node_id, MEMBERS)
        for node_id in MEMBERS
    }
    leader = nodes["node-1"]
    leader.state.current_term = 1
    leader.state.role = NodeRole.LEADER
    leader.state.leader_id = leader.node_id

    transport = InMemoryTransport()
    for node_id, node in nodes.items():
        transport.register(node_id, node)

    return leader, nodes, transport


class TransferTransport:
    def __init__(self, inner: InMemoryTransport) -> None:
        self.inner = inner

    def append_entries(
        self,
        target_id: str,
        request: AppendEntriesRequest,
    ) -> AppendEntriesResponse:
        return self.inner.append_entries(target_id, request)


class FailingTransferTransport(TransferTransport):
    def timeout_now(
        self,
        target_id: str,
        request: TimeoutNowRequest,
    ) -> TimeoutNowResponse:
        raise TransportError(f"{target_id} rejected transfer")


class BlockingTransferTransport(TransferTransport):
    def __init__(self, inner: InMemoryTransport) -> None:
        super().__init__(inner)
        self.timeout_started = Event()
        self.release_timeout = Event()

    def timeout_now(
        self,
        target_id: str,
        request: TimeoutNowRequest,
    ) -> TimeoutNowResponse:
        self.timeout_started.set()
        self.release_timeout.wait(timeout=1.0)
        return self.inner.timeout_now(target_id, request)


class HigherTermTransferTransport(TransferTransport):
    def timeout_now(
        self,
        target_id: str,
        request: TimeoutNowRequest,
    ) -> TimeoutNowResponse:
        return TimeoutNowResponse(
            term=request.term + 1,
            accepted=False,
        )


def test_leadership_transfers_to_caught_up_follower():
    leader, nodes, transport = create_leader()
    assert leader.put("before", "transfer", transport)

    transferred = leader.transfer_leadership(
        transport,
        target_id="node-2",
    )

    assert transferred is True
    assert leader.state.role == NodeRole.FOLLOWER
    assert nodes["node-2"].timer.expired() is True

    nodes["node-2"].tick(transport)

    assert nodes["node-2"].state.role == NodeRole.LEADER
    assert nodes["node-2"].put("after", "transfer", transport)
    assert nodes["node-2"].store.get("before") == "transfer"
    assert leader.store.get("after") == "transfer"


def test_transfer_failure_preserves_leader_and_writes():
    leader, _, inner = create_leader()
    transport = FailingTransferTransport(inner)

    assert leader.transfer_leadership(
        transport,
        target_id="node-2",
    ) is False

    assert leader.state.role == NodeRole.LEADER
    assert leader.put("still", "leader", transport) is True


def test_transfer_rejects_new_writes_while_in_progress():
    leader, _, inner = create_leader()
    transport = BlockingTransferTransport(inner)
    results: list[bool] = []

    thread = Thread(
        target=lambda: results.append(
            leader.transfer_leadership(
                transport,
                target_id="node-2",
            )
        )
    )
    thread.start()

    assert transport.timeout_started.wait(timeout=0.5)

    with pytest.raises(LeadershipTransferInProgressError):
        leader.put("rejected", "value", transport)

    transport.release_timeout.set()
    thread.join(timeout=1.0)

    assert not thread.is_alive()
    assert results == [True]
    assert leader.store.get("rejected") is None


def test_unreachable_target_does_not_force_step_down():
    leader, _, transport = create_leader()
    transport.block("node-2")

    assert leader.transfer_leadership(
        transport,
        target_id="node-2",
    ) is False
    assert leader.state.role == NodeRole.LEADER


def test_higher_term_transfer_response_steps_leader_down():
    leader, _, inner = create_leader()
    transport = HigherTermTransferTransport(inner)

    assert leader.transfer_leadership(
        transport,
        target_id="node-2",
    ) is False

    assert leader.state.role == NodeRole.FOLLOWER
    assert leader.state.current_term == 2


def test_follower_cannot_transfer_leadership():
    _, nodes, transport = create_leader()

    with pytest.raises(NotLeaderError):
        nodes["node-2"].transfer_leadership(transport)


def test_timeout_now_requires_current_leader():
    _, nodes, _ = create_leader()
    follower = nodes["node-2"]
    follower.state.current_term = 1

    response = follower.handle_timeout_now(
        TimeoutNowRequest(
            term=1,
            leader_id="node-1",
        )
    )

    assert response.accepted is False
    assert follower.timer.expired() is False


def test_timeout_now_expires_timer_after_leader_contact():
    leader, nodes, transport = create_leader()
    leader.send_heartbeats(transport)
    follower = nodes["node-2"]

    response = follower.handle_timeout_now(
        TimeoutNowRequest(
            term=1,
            leader_id="node-1",
        )
    )

    assert response.accepted is True
    assert follower.timer.expired() is True
