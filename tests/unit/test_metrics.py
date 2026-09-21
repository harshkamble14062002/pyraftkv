from pyraftkv.observability.metrics import RaftMetrics
from pyraftkv.raft.node import RaftNode


def test_metrics_render_contains_raft_metrics(tmp_path):
    node = RaftNode(
        node_id="node1",
        members={"node1"},
        data_dir=tmp_path,
    )

    metrics = RaftMetrics(
        node_id="node1",
    )

    metrics.update_from_node(node)

    output = metrics.render().decode()

    assert "pyraftkv_raft_current_term" in output
    assert "pyraftkv_raft_commit_index" in output
    assert "pyraftkv_raft_last_applied" in output
    assert "pyraftkv_raft_log_last_index" in output
    assert 'node_id="node1"' in output