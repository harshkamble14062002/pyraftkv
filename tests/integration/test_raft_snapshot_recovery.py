from pyraftkv.raft.log import (
    LogEntry,
    RaftCommand,
)
from pyraftkv.raft.node import RaftNode
from pyraftkv.raft.persistence import RaftPersistence
from pyraftkv.raft.snapshot import RaftSnapshot
from pyraftkv.raft.state import NodeRole, RaftState
from pyraftkv.transport.memory import InMemoryTransport


def make_single_node(tmp_path) -> RaftNode:
    node = RaftNode(
        "node-1",
        {"node-1"},
        data_dir=tmp_path,
    )
    node.state.current_term = 1
    node.state.role = NodeRole.LEADER
    node.state.leader_id = node.node_id
    return node


def test_restart_after_local_compaction(tmp_path):
    node = make_single_node(tmp_path)
    transport = InMemoryTransport()

    assert node.put("a", "1", transport)
    assert node.put("b", "2", transport)
    assert node.create_snapshot() is True

    assert node.log.base_index == 2
    assert node.log.last_index == 2
    assert node.persistence is not None
    assert node.persistence.snapshot_path.exists()

    restarted = RaftNode(
        "node-1",
        {"node-1"},
        data_dir=tmp_path,
    )

    assert restarted.state.role == NodeRole.FOLLOWER
    assert restarted.log.base_index == 2
    assert restarted.log.base_term == 1
    assert restarted.state.commit_index == 2
    assert restarted.state.last_applied == 2
    assert restarted.store.snapshot() == {
        "a": "1",
        "b": "2",
    }


def test_snapshot_only_recovery_clamps_commit_index(
    tmp_path,
):
    persistence = RaftPersistence(tmp_path)
    persistence.save_state(
        RaftState(
            node_id="node-1",
            current_term=3,
            commit_index=0,
        )
    )
    persistence.save_snapshot(
        RaftSnapshot(
            last_included_index=5,
            last_included_term=2,
            state={"key": "value"},
        )
    )

    restarted = RaftNode(
        "node-1",
        {"node-1"},
        data_dir=tmp_path,
    )

    assert restarted.log.base_index == 5
    assert restarted.state.commit_index == 5
    assert restarted.state.last_applied == 5
    assert restarted.store.get("key") == "value"
    assert persistence.load_state(
        "node-1"
    ).commit_index == 5


def test_recovery_after_snapshot_before_log_compaction(
    tmp_path,
):
    node = make_single_node(tmp_path)
    transport = InMemoryTransport()

    assert node.put("a", "1", transport)
    assert node.put("b", "2", transport)
    assert node.persistence is not None

    snapshot = RaftSnapshot(
        last_included_index=2,
        last_included_term=1,
        state=node.store.snapshot(),
    )

    # Simulate a crash immediately after snapshot persistence,
    # before the journal prefix is compacted.
    node.persistence.save_snapshot(snapshot)

    assert node.persistence.load_log().base_index == 0

    restarted = RaftNode(
        "node-1",
        {"node-1"},
        data_dir=tmp_path,
    )

    assert restarted.log.base_index == 2
    assert restarted.store.snapshot() == {
        "a": "1",
        "b": "2",
    }

    # Startup reconciles the journal, so another restart is
    # also independent of the removed prefix.
    assert RaftPersistence(
        tmp_path
    ).load_log().base_index == 2


def test_snapshot_plus_committed_suffix_recovery(
    tmp_path,
):
    persistence = RaftPersistence(tmp_path)
    persistence.save_state(
        RaftState(
            node_id="node-1",
            current_term=2,
            commit_index=2,
        )
    )
    persistence.save_snapshot(
        RaftSnapshot(
            last_included_index=1,
            last_included_term=1,
            state={"a": "1"},
        )
    )
    persistence.append_log_entries(
        [
            LogEntry(
                index=1,
                term=1,
                command=RaftCommand(
                    operation="PUT",
                    key="a",
                    value="1",
                ),
            ),
            LogEntry(
                index=2,
                term=2,
                command=RaftCommand(
                    operation="PUT",
                    key="b",
                    value="2",
                ),
            ),
        ]
    )

    restarted = RaftNode(
        "node-1",
        {"node-1"},
        data_dir=tmp_path,
    )

    assert restarted.log.base_index == 1
    assert restarted.log.get(2) is not None
    assert restarted.state.last_applied == 2
    assert restarted.store.snapshot() == {
        "a": "1",
        "b": "2",
    }


def test_append_only_persistence_continues_after_compaction(
    tmp_path,
):
    node = make_single_node(tmp_path)
    transport = InMemoryTransport()

    assert node.put("a", "1", transport)
    assert node.create_snapshot()
    assert node.put("b", "2", transport)

    restored_log = RaftPersistence(
        tmp_path
    ).load_log()

    assert restored_log.base_index == 1
    assert restored_log.get(2) is not None
