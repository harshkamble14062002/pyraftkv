from pyraftkv.raft.state import NodeRole
from tests.helpers.raft_cluster import RaftCluster


def test_snapshot_catch_up_survives_follower_restart(
    tmp_path,
):
    with RaftCluster(data_dir=tmp_path) as cluster:
        leader = cluster.elect_leader("node-1")
        cluster.transport.block("node-3")

        for index in range(1, 5):
            assert leader.put(
                f"snapshot-{index}",
                str(index),
                cluster.transport,
            )

        assert leader.create_snapshot()
        snapshot_index = leader.snapshot.last_included_index

        assert leader.put(
            "suffix",
            "retained",
            cluster.transport,
        )

        lagging = cluster.nodes["node-3"]
        cluster.transport.unblock("node-3")
        cluster.run_until(
            lambda: (
                lagging.store.snapshot()
                == leader.store.snapshot()
                and lagging.state.commit_index
                == leader.state.commit_index
            )
        )

        assert cluster.transport.call_count(
            "install_snapshot",
            "node-1",
            "node-3",
        ) >= 1
        assert lagging.log.base_index == snapshot_index
        assert lagging.state.last_applied == (
            lagging.state.commit_index
        )

        restarted = cluster.restart_node("node-3")

        assert restarted.state.role == NodeRole.FOLLOWER
        assert restarted.store.snapshot() == (
            leader.store.snapshot()
        )
        assert restarted.state.commit_index == (
            leader.state.commit_index
        )
        assert restarted.state.last_applied == (
            leader.state.last_applied
        )


def test_failed_snapshot_transfer_retries_without_partial_state(
    tmp_path,
):
    with RaftCluster(data_dir=tmp_path) as cluster:
        leader = cluster.elect_leader("node-1")
        cluster.transport.block("node-3")

        for index in range(1, 4):
            assert leader.put(
                f"key-{index}",
                str(index),
                cluster.transport,
            )

        assert leader.create_snapshot()
        lagging = cluster.nodes["node-3"]
        cluster.transport.unblock("node-3")
        cluster.transport.block_link(
            "node-1",
            "node-2",
        )
        cluster.transport.drop_install_snapshot(
            "node-1",
            "node-3",
        )

        leader.replicate_log(cluster.transport)

        assert cluster.transport.call_count(
            "install_snapshot",
            "node-1",
            "node-3",
        ) == 1
        assert lagging.log.base_index == 0
        assert lagging.store.snapshot() == {}
        assert leader.state.role == NodeRole.LEADER

        cluster.transport.unblock_link(
            "node-1",
            "node-2",
        )
        assert leader.put(
            "after-failure",
            "committed",
            cluster.transport,
        )

        cluster.run_until(
            lambda: (
                lagging.store.snapshot()
                == leader.store.snapshot()
                and lagging.state.commit_index
                == leader.state.commit_index
            )
        )

        assert cluster.transport.call_count(
            "install_snapshot",
            "node-1",
            "node-3",
        ) >= 2
        assert lagging.state.last_applied == (
            lagging.state.commit_index
        )
        cluster.assert_converged()
