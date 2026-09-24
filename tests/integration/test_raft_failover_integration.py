from threading import Event, Thread
from unittest.mock import Mock

from pyraftkv.raft.node import RaftNode
from pyraftkv.raft.rpc import (
    RequestVoteRequest,
    RequestVoteResponse,
)
from pyraftkv.raft.state import NodeRole
from pyraftkv.transport.base import TransportError
from pyraftkv.transport.memory import InMemoryTransport

MEMBERS = {
    "node-1",
    "node-2",
    "node-3",
}


class BlockingVoteTransport:
    """Simulate an outbound RequestVote RPC stuck on a failed peer."""

    def __init__(self) -> None:
        self.request_started = Event()
        self.release_request = Event()

    def request_vote(
        self,
        target_id,
        request,
    ):
        self.request_started.set()

        self.release_request.wait(
            timeout=2.0,
        )

        raise TransportError(
            f"{target_id} is unavailable"
        )

class DeadPeerFirstTransport:
    """Simulate one unavailable peer and one healthy voter."""

    def __init__(self) -> None:
        self.dead_request_started = Event()
        self.healthy_request_started = Event()
        self.release_dead_request = Event()

    def request_vote(
        self,
        target_id,
        request,
    ):
        if target_id == "node-1":
            self.dead_request_started.set()

            self.release_dead_request.wait(
                timeout=2.0,
            )

            raise TransportError(
                "node-1 is unavailable"
            )

        if target_id == "node-3":
            # Make the scenario deterministic: the healthy
            # voter responds only after the unavailable peer's
            # RPC is confirmed to be in flight.
            if not self.dead_request_started.wait(
                timeout=1.0,
            ):
                raise TransportError(
                    "dead-peer RPC did not start"
                )

            self.healthy_request_started.set()

            return RequestVoteResponse(
                term=request.term,
                vote_granted=True,
            )

class DeadFollowerTransport:
    """Simulate one dead follower and one healthy follower."""

    def __init__(self) -> None:
        self.dead_started = Event()
        self.healthy_started = Event()
        self.release_dead = Event()

    def append_entries(
        self,
        target_id,
        request,
    ):
        if target_id == "node-1":
            self.dead_started.set()

            self.release_dead.wait(
                timeout=2.0,
            )

            raise TransportError(
                "node-1 is unavailable"
            )

        if target_id == "node-3":
            self.healthy_started.set()

            return Mock(
                term=request.term,
                success=True,
            )

        raise TransportError(
            f"Unexpected target: {target_id}"
        )

def create_cluster():
    transport = InMemoryTransport()

    nodes = {
        node_id: RaftNode(
            node_id,
            MEMBERS,
        )
        for node_id in MEMBERS
    }

    for node_id, node in nodes.items():
        transport.register(
            node_id,
            node,
        )

    return transport, nodes


def elect(
    node: RaftNode,
    transport: InMemoryTransport,
) -> None:
    node.timer = Mock()
    node.timer.expired.return_value = True

    node.tick(transport)

    assert node.state.role == NodeRole.LEADER


def test_new_leader_accepts_write_after_failover():
    transport, nodes = create_cluster()

    node1 = nodes["node-1"]

    elect(
        node1,
        transport,
    )

    assert node1.put(
        "before",
        "failure",
        transport,
    )

    transport.block(
        "node-1"
    )

    node2 = nodes["node-2"]

    elect(
        node2,
        transport,
    )

    committed = node2.put(
        "after",
        "failover",
        transport,
    )

    assert committed is True

    assert (
        node2.store.get("before")
        == "failure"
    )

    assert (
        node2.store.get("after")
        == "failover"
    )

    assert (
        nodes["node-3"].store.get(
            "before"
        )
        == "failure"
    )

    assert (
        nodes["node-3"].store.get(
            "after"
        )
        == "failover"
    )


def test_restarted_node_catches_up_from_new_leader():
    transport, nodes = create_cluster()

    old_leader = nodes["node-1"]

    elect(
        old_leader,
        transport,
    )

    assert old_leader.put(
        "before",
        "failure",
        transport,
    )

    # Simulate node-1 crashing.
    transport.block(
        "node-1"
    )

    new_leader = nodes["node-2"]

    elect(
        new_leader,
        transport,
    )

    assert new_leader.put(
        "after",
        "failover",
        transport,
    )

    # Replace the dead process with a brand-new RaftNode.
    restarted = RaftNode(
        "node-1",
        MEMBERS,
    )

    transport.unregister(
        "node-1"
    )

    transport.register(
        "node-1",
        restarted,
    )

    transport.unblock(
        "node-1"
    )

    # Periodic leader replication should repair it.
    new_leader.replicate_log(
        transport
    )

    assert (
        restarted.state.role
        == NodeRole.FOLLOWER
    )

    assert (
        restarted.state.current_term
        == new_leader.state.current_term
    )

    assert (
        restarted.log.last_index
        == new_leader.log.last_index
    )

    assert (
        restarted.store.get("before")
        == "failure"
    )

    assert (
        restarted.store.get("after")
        == "failover"
    )


def test_inbound_vote_is_not_blocked_by_outbound_vote_rpc():
    members = {
        "node-1",
        "node-2",
    }

    node = RaftNode(
        "node-1",
        members,
    )

    node.timer = Mock()
    node.timer.expired.return_value = True
    node.timer.reset = Mock()

    transport = BlockingVoteTransport()

    tick_thread = Thread(
        target=node.tick,
        args=(transport,),
    )

    tick_thread.start()

    assert transport.request_started.wait(
        timeout=1.0,
    )

    election_term = (
        node.state.current_term
    )

    incoming_request = RequestVoteRequest(
        term=election_term + 1,
        candidate_id="node-2",
        last_log_index=node.log.last_index,
        last_log_term=node.log.last_term,
    )

    responses = []

    def handle_incoming_vote():
        responses.append(
            node.handle_request_vote(
                incoming_request,
            )
        )

    inbound_thread = Thread(
        target=handle_incoming_vote,
    )

    inbound_thread.start()

    # An outbound network call must not prevent this node
    # from processing an inbound Raft RPC.
    inbound_thread.join(
        timeout=0.25,
    )

    inbound_completed = (
        not inbound_thread.is_alive()
    )

    # Always release the fake blocked RPC so no test thread
    # is left running after the assertion.
    transport.release_request.set()

    tick_thread.join(
        timeout=1.0,
    )

    inbound_thread.join(
        timeout=1.0,
    )

    assert inbound_completed is True

    assert responses

    assert (
        node.state.role
        == NodeRole.FOLLOWER
    )

    assert (
        node.state.current_term
        == election_term + 1
    )

def test_dead_peer_does_not_delay_vote_from_healthy_peer():
    members = {
        "node-1",
        "node-2",
        "node-3",
    }

    node = RaftNode(
        "node-2",
        members,
    )

    node.timer = Mock()
    node.timer.expired.return_value = True
    node.timer.reset = Mock()

    transport = DeadPeerFirstTransport()

    tick_thread = Thread(
        target=node.tick,
        args=(transport,),
    )

    tick_thread.start()

    assert transport.dead_request_started.wait(
        timeout=1.0,
    )

    healthy_request_completed = (
        transport.healthy_request_started.wait(
            timeout=0.25,
        )
    )

    # Always release the dead-peer RPC before asserting so
    # the test never leaves a worker thread behind.
    transport.release_dead_request.set()

    tick_thread.join(
        timeout=1.0,
    )

    assert healthy_request_completed is True

    assert (
        node.state.role
        == NodeRole.LEADER
    )

def test_dead_follower_does_not_delay_healthy_follower_replication():
    members = {
        "node-1",
        "node-2",
        "node-3",
    }

    leader = RaftNode(
        "node-2",
        members,
    )

    leader.state.current_term = 1
    leader.state.role = NodeRole.LEADER
    leader.state.leader_id = "node-2"

    transport = DeadFollowerTransport()

    replication_thread = Thread(
        target=leader.replicate_log,
        args=(transport,),
    )

    replication_thread.start()

    assert transport.dead_started.wait(
        timeout=1.0,
    )

    healthy_completed = (
        transport.healthy_started.wait(
            timeout=0.25,
        )
    )

    transport.release_dead.set()

    replication_thread.join(
        timeout=1.0,
    )

    assert healthy_completed is True