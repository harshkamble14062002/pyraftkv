from unittest.mock import Mock

import pytest

from pyraftkv.raft.log import (
    LogEntry,
    RaftCommand,
)
from pyraftkv.raft.node import RaftNode
from pyraftkv.raft.rpc import (
    AppendEntriesRequest,
    InstallSnapshotRequest,
)
from pyraftkv.raft.state import NodeRole
from pyraftkv.transport.memory import InMemoryTransport

MEMBERS = {"node-1", "node-2", "node-3"}


def create_cluster(tmp_path=None):
    transport = InMemoryTransport()
    nodes = {
        node_id: RaftNode(
            node_id,
            MEMBERS,
            data_dir=(
                tmp_path / node_id
                if tmp_path is not None
                else None
            ),
        )
        for node_id in MEMBERS
    }

    for node_id, node in nodes.items():
        transport.register(node_id, node)

    leader = nodes["node-1"]
    leader.state.current_term = 1
    leader.state.role = NodeRole.LEADER
    leader.state.leader_id = leader.node_id

    return transport, nodes, leader


def test_follower_installs_snapshot(tmp_path):
    follower = RaftNode(
        "node-2",
        MEMBERS,
        data_dir=tmp_path,
    )

    response = follower.handle_install_snapshot(
        InstallSnapshotRequest(
            term=2,
            leader_id="node-1",
            last_included_index=5,
            last_included_term=2,
            state={
                "language": "python",
            },
        )
    )

    assert response.success is True
    assert follower.state.role == NodeRole.FOLLOWER
    assert follower.state.current_term == 2
    assert follower.state.commit_index == 5
    assert follower.state.last_applied == 5
    assert follower.log.base_index == 5
    assert follower.log.base_term == 2
    assert follower.store.get("language") == "python"


def test_higher_term_snapshot_steps_leader_down(tmp_path):
    node = RaftNode(
        "node-2",
        MEMBERS,
        data_dir=tmp_path,
    )
    node.state.current_term = 1
    node.state.role = NodeRole.LEADER
    node.state.leader_id = node.node_id

    response = node.handle_install_snapshot(
        InstallSnapshotRequest(
            term=2,
            leader_id="node-1",
            last_included_index=3,
            last_included_term=1,
            state={"key": "value"},
        )
    )

    assert response.success is True
    assert node.state.role == NodeRole.FOLLOWER
    assert node.state.current_term == 2
    assert node.persistence is not None
    assert node.persistence.load_state(
        "node-2"
    ).current_term == 2


def test_stale_snapshot_is_rejected(tmp_path):
    follower = RaftNode(
        "node-2",
        MEMBERS,
        data_dir=tmp_path,
    )
    follower.state.current_term = 3
    follower.store.put("key", "current")

    response = follower.handle_install_snapshot(
        InstallSnapshotRequest(
            term=2,
            leader_id="node-1",
            last_included_index=5,
            last_included_term=2,
            state={"key": "stale"},
        )
    )

    assert response.success is False
    assert response.term == 3
    assert follower.store.get("key") == "current"
    assert follower.log.base_index == 0


def test_append_entries_resume_after_snapshot():
    follower = RaftNode("node-2", MEMBERS)

    snapshot_response = follower.handle_install_snapshot(
        InstallSnapshotRequest(
            term=2,
            leader_id="node-1",
            last_included_index=2,
            last_included_term=1,
            state={"a": "1", "b": "2"},
        )
    )
    append_response = follower.handle_append_entries(
        AppendEntriesRequest(
            term=2,
            leader_id="node-1",
            prev_log_index=2,
            prev_log_term=1,
            entries=(
                LogEntry(
                    index=3,
                    term=2,
                    command=RaftCommand(
                        operation="PUT",
                        key="c",
                        value="3",
                    ),
                ),
            ),
            leader_commit=3,
        )
    )

    assert snapshot_response.success is True
    assert append_response.success is True
    assert follower.log.get(3) is not None
    assert follower.store.snapshot() == {
        "a": "1",
        "b": "2",
        "c": "3",
    }


def test_leader_sends_snapshot_then_remaining_suffix():
    transport, nodes, leader = create_cluster()
    transport.block("node-3")

    assert leader.put("a", "1", transport)
    assert leader.put("b", "2", transport)
    assert leader.create_snapshot()
    assert leader.put("c", "3", transport)

    lagging = nodes["node-3"]
    assert lagging.log.last_index == 0

    transport.unblock("node-3")
    leader.replicate_log(transport)

    assert lagging.log.base_index == 2
    assert lagging.log.get(3) == leader.log.get(3)
    assert lagging.store.snapshot() == {
        "a": "1",
        "b": "2",
        "c": "3",
    }
    assert leader.replication is not None
    assert leader.replication.match_index["node-3"] == 3
    assert leader.replication.next_index["node-3"] == 4


def test_restart_after_snapshot_installation(tmp_path):
    transport, _nodes, leader = create_cluster(tmp_path)
    transport.block("node-3")

    assert leader.put("a", "1", transport)
    assert leader.put("b", "2", transport)
    assert leader.create_snapshot()
    assert leader.put("c", "3", transport)

    transport.unblock("node-3")
    leader.replicate_log(transport)

    restarted = RaftNode(
        "node-3",
        MEMBERS,
        data_dir=tmp_path / "node-3",
    )

    assert restarted.state.role == NodeRole.FOLLOWER
    assert restarted.log.base_index == 2
    assert restarted.log.get(3) is not None
    assert restarted.state.commit_index == 3
    assert restarted.state.last_applied == 3
    assert restarted.store.snapshot() == {
        "a": "1",
        "b": "2",
        "c": "3",
    }


def test_snapshot_is_durable_before_success(tmp_path):
    follower = RaftNode(
        "node-2",
        MEMBERS,
        data_dir=tmp_path,
    )
    real_persistence = follower.persistence
    assert real_persistence is not None
    follower.persistence = Mock(
        wraps=real_persistence
    )

    response = follower.handle_install_snapshot(
        InstallSnapshotRequest(
            term=2,
            leader_id="node-1",
            last_included_index=5,
            last_included_term=2,
            state={"key": "value"},
        )
    )

    assert response.success is True
    assert [
        call[0]
        for call in follower.persistence.method_calls
    ][:3] == [
        "save_snapshot",
        "save_log",
        "save_state",
    ]


def test_snapshot_persistence_failure_is_not_acknowledged(
    tmp_path,
):
    follower = RaftNode(
        "node-2",
        MEMBERS,
        data_dir=tmp_path,
    )
    assert follower.persistence is not None
    follower.persistence.save_snapshot = Mock(
        side_effect=OSError("disk failed")
    )

    with pytest.raises(OSError):
        follower.handle_install_snapshot(
            InstallSnapshotRequest(
                term=2,
                leader_id="node-1",
                last_included_index=5,
                last_included_term=2,
                state={"key": "value"},
            )
        )

    assert follower.log.base_index == 0
    assert follower.store.get("key") is None
