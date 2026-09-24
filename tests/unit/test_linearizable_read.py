from threading import Event, Thread

import pytest

from pyraftkv.raft.log import RaftCommand
from pyraftkv.raft.node import (
    NotLeaderError,
    RaftNode,
    ReadQuorumError,
)
from pyraftkv.raft.rpc import (
    AppendEntriesRequest,
    AppendEntriesResponse,
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


def test_leader_can_read_with_all_peers_healthy():
    leader, _, transport = create_leader()
    leader.store.put("language", "python")

    assert leader.linearizable_get("language", transport) == "python"


def test_leader_can_read_with_one_follower_unavailable():
    leader, _, transport = create_leader()
    leader.store.put("language", "python")
    transport.block("node-3")

    assert leader.linearizable_get("language", transport) == "python"


def test_leader_cannot_read_without_follower_quorum():
    leader, _, transport = create_leader()
    leader.store.put("unsafe", "stale")
    transport.block("node-2")
    transport.block("node-3")

    with pytest.raises(ReadQuorumError):
        leader.linearizable_get("unsafe", transport)


def test_follower_cannot_serve_linearizable_read():
    _, nodes, transport = create_leader()
    follower = nodes["node-2"]

    with pytest.raises(NotLeaderError):
        follower.linearizable_get("key", transport)


def test_read_barrier_does_not_append_log_entry():
    leader, _, transport = create_leader()
    before = tuple(leader.log.entries_from(1))

    leader.linearizable_get("missing", transport)

    assert tuple(leader.log.entries_from(1)) == before


class HigherTermTransport:
    def append_entries(
        self,
        target_id: str,
        request: AppendEntriesRequest,
    ) -> AppendEntriesResponse:
        if target_id == "node-2":
            return AppendEntriesResponse(
                term=request.term + 1,
                success=False,
            )
        raise TransportError(f"{target_id} is unavailable")


def test_higher_term_response_steps_down_and_fails_read():
    leader, _, _ = create_leader()

    with pytest.raises(ReadQuorumError):
        leader.linearizable_get("key", HigherTermTransport())

    assert leader.state.role == NodeRole.FOLLOWER
    assert leader.state.current_term == 2


class BlockingReadTransport:
    def __init__(self) -> None:
        self.started = Event()
        self.release = Event()

    def append_entries(
        self,
        target_id: str,
        request: AppendEntriesRequest,
    ) -> AppendEntriesResponse:
        self.started.set()
        self.release.wait(timeout=1.0)
        return AppendEntriesResponse(
            term=request.term,
            success=True,
        )


def test_term_change_while_confirming_quorum_fails_read():
    leader, _, _ = create_leader()
    transport = BlockingReadTransport()
    errors: list[Exception] = []

    def read() -> None:
        try:
            leader.linearizable_get("key", transport)
        except Exception as exc:  # noqa: BLE001 - captured for thread assertion
            errors.append(exc)

    thread = Thread(target=read)
    thread.start()

    assert transport.started.wait(timeout=0.5)

    leader.handle_append_entries(
        AppendEntriesRequest(
            term=2,
            leader_id="node-2",
        )
    )
    transport.release.set()
    thread.join(timeout=1.0)

    assert not thread.is_alive()
    assert errors
    assert isinstance(errors[0], ReadQuorumError)


def test_read_applies_committed_state_before_returning_value():
    leader, _, transport = create_leader()
    leader.log.append(
        term=1,
        command=RaftCommand(
            operation="PUT",
            key="committed",
            value="value",
        ),
    )
    leader.state.commit_index = 1

    assert leader.state.last_applied == 0
    assert leader.linearizable_get("committed", transport) == "value"
    assert leader.state.last_applied == 1




def test_role_change_while_confirming_quorum_fails_read():
    leader, _, _ = create_leader()
    transport = BlockingReadTransport()
    errors: list[Exception] = []

    def read() -> None:
        try:
            leader.linearizable_get("key", transport)
        except Exception as exc:  # noqa: BLE001 - captured for thread assertion
            errors.append(exc)

    thread = Thread(target=read)
    thread.start()

    assert transport.started.wait(timeout=0.5)

    leader.handle_append_entries(
        AppendEntriesRequest(
            term=1,
            leader_id="node-2",
        )
    )
    transport.release.set()
    thread.join(timeout=1.0)

    assert not thread.is_alive()
    assert errors
    assert isinstance(errors[0], ReadQuorumError)


def test_stale_follower_value_cannot_be_read_authoritatively():
    _, nodes, transport = create_leader()
    follower = nodes["node-2"]
    follower.store.put("key", "stale")

    with pytest.raises(NotLeaderError):
        follower.linearizable_get("key", transport)


class CountingTransport:
    def __init__(self, inner: InMemoryTransport) -> None:
        self.inner = inner
        self.append_targets: list[str] = []

    def append_entries(
        self,
        target_id: str,
        request: AppendEntriesRequest,
    ) -> AppendEntriesResponse:
        self.append_targets.append(target_id)
        return self.inner.append_entries(target_id, request)


def test_recent_quorum_confirmation_is_reused(monkeypatch):
    now = [10.0]
    monkeypatch.setattr(
        "pyraftkv.raft.node.monotonic",
        lambda: now[0],
    )
    leader, _, inner = create_leader()
    transport = CountingTransport(inner)
    leader.store.put("key", "value")

    assert leader.linearizable_get("key", transport) == "value"
    first_barrier_calls = len(transport.append_targets)

    assert leader.linearizable_get("key", transport) == "value"
    assert len(transport.append_targets) == first_barrier_calls


def test_expired_read_lease_requires_fresh_quorum(monkeypatch):
    now = [10.0]
    monkeypatch.setattr(
        "pyraftkv.raft.node.monotonic",
        lambda: now[0],
    )
    leader, _, inner = create_leader()
    transport = CountingTransport(inner)

    leader.linearizable_get("key", transport)
    first_barrier_calls = len(transport.append_targets)

    now[0] += leader._read_lease_duration

    leader.linearizable_get("key", transport)

    assert len(transport.append_targets) > first_barrier_calls


def test_term_change_invalidates_read_lease(monkeypatch):
    now = [10.0]
    monkeypatch.setattr(
        "pyraftkv.raft.node.monotonic",
        lambda: now[0],
    )
    leader, _, inner = create_leader()
    transport = CountingTransport(inner)

    leader.linearizable_get("key", transport)
    first_barrier_calls = len(transport.append_targets)

    leader.state.current_term += 1

    leader.linearizable_get("key", transport)

    assert len(transport.append_targets) > first_barrier_calls
