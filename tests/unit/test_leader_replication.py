import pytest

from pyraftkv.raft.leader import LeaderReplication
from pyraftkv.raft.log import RaftCommand, RaftLog
from pyraftkv.raft.state import NodeRole, RaftState


def create_leader() -> RaftState:
    return RaftState(
        node_id="node-1",
        current_term=3,
        role=NodeRole.LEADER,
        leader_id="node-1",
    )


def populated_log() -> RaftLog:
    log = RaftLog()

    for value in ("a", "b", "c"):
        log.append(
            term=3,
            command=RaftCommand(
                operation="PUT",
                key=value,
                value=value,
            ),
        )

    return log


def test_replication_indexes_are_initialized():
    state = create_leader()
    log = populated_log()

    replication = LeaderReplication(
        state,
        log,
        {"node-1", "node-2", "node-3"},
    )

    assert replication.next_index == {
        "node-2": 4,
        "node-3": 4,
    }

    assert replication.match_index == {
        "node-2": 0,
        "node-3": 0,
    }

def test_build_request_uses_next_index():
    state = create_leader()
    log = populated_log()

    replication = LeaderReplication(
        state,
        log,
        {"node-1", "node-2"},
    )

    replication.next_index["node-2"] = 2

    request = replication.build_request("node-2")

    assert request.prev_log_index == 1
    assert request.prev_log_term == 3

    assert [
        entry.index
        for entry in request.entries
    ] == [2, 3]

def test_success_advances_replication_progress():
    state = create_leader()
    log = populated_log()

    replication = LeaderReplication(
        state,
        log,
        {"node-1", "node-2"},
    )

    replication.next_index["node-2"] = 2

    request = replication.build_request("node-2")

    replication.record_success(
        "node-2",
        request,
    )

    assert replication.match_index["node-2"] == 3
    assert replication.next_index["node-2"] == 4


def test_failure_moves_next_index_back():
    state = create_leader()
    log = populated_log()

    replication = LeaderReplication(
        state,
        log,
        {"node-1", "node-2"},
    )

    assert replication.next_index["node-2"] == 4

    replication.record_failure("node-2")

    assert replication.next_index["node-2"] == 3

def test_next_index_never_goes_below_one():
    state = create_leader()
    log = populated_log()

    replication = LeaderReplication(
        state,
        log,
        {"node-1", "node-2"},
    )

    replication.next_index["node-2"] = 1

    replication.record_failure("node-2")

    assert replication.next_index["node-2"] == 1

def test_unknown_follower_is_rejected():
    state = create_leader()
    log = populated_log()

    replication = LeaderReplication(
        state,
        log,
        {"node-1", "node-2"},
    )

    with pytest.raises(ValueError):
        replication.build_request("fake-node")
