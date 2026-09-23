from unittest.mock import ANY, Mock

import pytest

from pyraftkv.observability.metrics import RaftMetrics
from pyraftkv.raft.log import RaftCommand
from pyraftkv.raft.node import RaftNode
from pyraftkv.raft.rpc import InstallSnapshotRequest
from pyraftkv.raft.state import NodeRole
from pyraftkv.transport.memory import InMemoryTransport


def test_operational_metrics_render_expected_series():
    metrics = RaftMetrics("node-1")

    metrics.record_election_attempt()
    metrics.record_leadership_change()
    metrics.record_rpc(
        rpc="append_entries",
        duration_seconds=0.01,
        success=False,
    )
    metrics.record_read_barrier(
        duration_seconds=0.02,
        success=False,
    )
    metrics.record_snapshot_installation(
        duration_seconds=0.03,
        size_bytes=17,
    )
    metrics.record_log_compaction()

    output = metrics.render().decode()

    assert "pyraftkv_raft_election_attempts_total" in output
    assert "pyraftkv_raft_leadership_changes_total" in output
    assert 'pyraftkv_raft_rpc_failures_total{node_id="node-1",rpc="append_entries"} 1.0' in output
    assert 'pyraftkv_raft_rpc_duration_seconds_count{node_id="node-1",rpc="append_entries"} 1.0' in output
    assert "pyraftkv_raft_read_barrier_attempts_total" in output
    assert "pyraftkv_raft_read_barrier_failures_total" in output
    assert "pyraftkv_raft_read_barrier_duration_seconds_count" in output
    assert "pyraftkv_raft_snapshot_installations_total" in output
    assert "pyraftkv_raft_snapshot_install_bytes_total" in output
    assert "pyraftkv_raft_snapshot_install_duration_seconds_count" in output
    assert "pyraftkv_raft_log_compactions_total" in output


def test_rpc_metric_rejects_unbounded_label_values():
    metrics = RaftMetrics("node-1")

    with pytest.raises(ValueError, match="Unsupported Raft RPC"):
        metrics.record_rpc(
            rpc="key-supplied-value",
            duration_seconds=0.01,
            success=True,
        )


def test_follower_lag_series_are_removed_after_step_down():
    node = RaftNode(
        "node-1",
        {"node-1", "node-2"},
    )
    node.state.role = NodeRole.LEADER
    node.state.current_term = 2
    node.state.leader_id = node.node_id
    node.log.append(
        term=2,
        command=RaftCommand(
            operation="PUT",
            key="language",
            value="python",
        ),
    )
    node._initialize_leader_replication()
    metrics = RaftMetrics(node.node_id)

    metrics.update_from_node(node)

    output = metrics.render().decode()
    assert 'follower_id="node-2"' in output

    node.state.role = NodeRole.FOLLOWER
    node.replication = None
    metrics.update_from_node(node)

    output = metrics.render().decode()
    assert 'follower_id="node-2"' not in output


def test_node_emits_election_read_snapshot_and_compaction_events():
    node = RaftNode("node-1", {"node-1"})
    observer = Mock()
    node.set_observer(observer)
    node.timer = Mock()
    node.timer.expired.return_value = True
    transport = InMemoryTransport()

    assert node.tick(transport) == NodeRole.LEADER
    observer.record_election_attempt.assert_called_once_with()
    observer.record_leadership_change.assert_called_once_with()

    node.confirm_read_quorum(transport)
    observer.record_read_barrier.assert_called_once_with(
        duration_seconds=ANY,
        success=True,
    )

    assert node.put("language", "python", transport)
    assert node.create_snapshot()
    observer.record_log_compaction.assert_called_once_with()

    response = node.handle_install_snapshot(
        InstallSnapshotRequest(
            term=node.state.current_term,
            leader_id="node-2",
            last_included_index=(
                node.snapshot.last_included_index
            ),
            last_included_term=(
                node.snapshot.last_included_term
            ),
            state=node.snapshot.state,
        )
    )

    assert response.success is True
    observer.record_snapshot_installation.assert_called_once_with(
        duration_seconds=ANY,
        size_bytes=21,
    )
