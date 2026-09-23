from time import perf_counter

import pytest

from pyraftkv.raft.node import (
    NotLeaderError,
    ReadQuorumError,
)
from pyraftkv.raft.rpc import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    InstallSnapshotRequest,
    InstallSnapshotResponse,
)
from pyraftkv.raft.state import NodeRole
from pyraftkv.transport.base import TransportError
from tests.helpers.raft_cluster import RaftCluster


class HigherTermAppendTransport:
    def append_entries(
        self,
        target_id: str,
        request: AppendEntriesRequest,
    ) -> AppendEntriesResponse:
        return AppendEntriesResponse(
            term=request.term + 1,
            success=False,
        )


class HigherTermSnapshotTransport:
    def __init__(self) -> None:
        self.install_calls = 0

    def append_entries(
        self,
        target_id: str,
        request: AppendEntriesRequest,
    ) -> AppendEntriesResponse:
        raise TransportError(
            f"{target_id} requires a snapshot"
        )

    def install_snapshot(
        self,
        target_id: str,
        request: InstallSnapshotRequest,
    ) -> InstallSnapshotResponse:
        self.install_calls += 1

        return InstallSnapshotResponse(
            term=request.term + 1,
            success=False,
        )


def test_follower_failure_preserves_quorum_progress():
    with RaftCluster() as cluster:
        leader = cluster.elect_leader("node-1")
        initial_term = leader.state.current_term
        cluster.transport.block("node-3")

        started = perf_counter()
        assert leader.put(
            "available",
            "yes",
            cluster.transport,
        )
        elapsed = perf_counter() - started

        assert elapsed < 0.5
        assert leader.state.role == NodeRole.LEADER
        assert leader.state.current_term == initial_term
        assert leader.linearizable_get(
            "available",
            cluster.transport,
        ) == "yes"
        assert cluster.nodes["node-2"].store.get(
            "available"
        ) == "yes"
        assert leader.state.commit_index == 1
        assert cluster.transport.call_count(
            "append_entries",
            "node-1",
            "node-2",
        ) >= 1
        cluster.assert_invariants()


def test_isolated_leader_cannot_commit_or_read_after_lease(
    monkeypatch,
):
    now = [10.0]
    monkeypatch.setattr(
        "pyraftkv.raft.node.monotonic",
        lambda: now[0],
    )

    with RaftCluster() as cluster:
        old_leader = cluster.elect_leader("node-1")
        assert old_leader.put(
            "committed",
            "before",
            cluster.transport,
        )
        assert old_leader.linearizable_get(
            "committed",
            cluster.transport,
        ) == "before"

        cluster.isolate("node-1")
        now[0] += old_leader._read_lease_duration

        assert old_leader.put(
            "minority",
            "unsafe",
            cluster.transport,
        ) is False

        with pytest.raises(ReadQuorumError):
            old_leader.linearizable_get(
                "committed",
                cluster.transport,
            )

        new_leader = cluster.elect_leader("node-2")

        assert new_leader.put(
            "majority",
            "safe",
            cluster.transport,
        )
        assert new_leader.linearizable_get(
            "committed",
            cluster.transport,
        ) == "before"
        assert new_leader.store.get("minority") is None
        assert old_leader.state.role == NodeRole.LEADER
        assert new_leader.state.current_term > (
            old_leader.state.current_term
        )
        cluster.assert_invariants()


def test_five_node_majority_partition_controls_progress():
    with RaftCluster(size=5) as cluster:
        leader = cluster.elect_leader("node-1")
        cluster.partition(
            {"node-1", "node-2", "node-3"},
            {"node-4", "node-5"},
        )

        assert leader.put(
            "majority",
            "committed",
            cluster.transport,
        )
        assert leader.linearizable_get(
            "majority",
            cluster.transport,
        ) == "committed"

        minority_candidate = cluster.nodes["node-4"]
        minority_candidate.timer.expire_now()
        minority_candidate.tick(cluster.transport)

        assert (
            minority_candidate.state.role
            != NodeRole.LEADER
        )

        with pytest.raises(NotLeaderError):
            minority_candidate.put(
                "minority",
                "rejected",
                cluster.transport,
            )

        with pytest.raises(NotLeaderError):
            minority_candidate.linearizable_get(
                "majority",
                cluster.transport,
            )

        assert (
            cluster.nodes["node-5"].store.get("minority")
            is None
        )
        cluster.assert_invariants()


def test_healing_repairs_divergent_persistent_log(
    tmp_path,
):
    with RaftCluster(data_dir=tmp_path) as cluster:
        old_leader = cluster.elect_leader("node-1")
        assert old_leader.put(
            "base",
            "committed",
            cluster.transport,
        )

        cluster.isolate("node-1")

        assert old_leader.put(
            "losing",
            "uncommitted",
            cluster.transport,
        ) is False

        conflicting_index = old_leader.log.last_index
        new_leader = cluster.elect_leader("node-2")
        assert new_leader.put(
            "winner",
            "committed",
            cluster.transport,
        )

        cluster.heal()
        cluster.run_until(
            lambda: all(
                node.store.get("winner") == "committed"
                and node.store.get("losing") is None
                and node.state.commit_index
                == new_leader.state.commit_index
                for node in cluster.nodes.values()
            )
        )

        repaired = old_leader.log.get(
            conflicting_index
        )
        winning = new_leader.log.get(
            conflicting_index
        )

        assert repaired == winning
        assert repaired is not None
        assert repaired.command.key == "winner"
        assert old_leader.state.role == NodeRole.FOLLOWER

        restarted = cluster.restart_node("node-1")

        assert restarted.store.get("winner") == "committed"
        assert restarted.store.get("losing") is None
        assert restarted.state.role == NodeRole.FOLLOWER
        cluster.assert_converged()


def test_delayed_request_vote_does_not_block_election():
    with RaftCluster() as cluster:
        release = cluster.transport.delay_request_vote(
            "node-1",
            "node-3",
        )

        try:
            started = perf_counter()
            leader = cluster.elect_leader("node-1")
            elapsed = perf_counter() - started

            assert elapsed < 0.2
            assert leader.state.role == NodeRole.LEADER
            assert cluster.transport.call_count(
                "request_vote",
                "node-1",
                "node-2",
            ) == 1
        finally:
            release.set()


def test_delayed_append_does_not_stall_replication_round():
    with RaftCluster() as cluster:
        leader = cluster.elect_leader("node-1")
        release = cluster.transport.delay_append_entries(
            "node-1",
            "node-3",
        )

        try:
            started = perf_counter()
            committed = leader.put(
                "bounded",
                "return",
                cluster.transport,
            )
            elapsed = perf_counter() - started

            assert committed is True
            assert elapsed < 0.2
            assert cluster.nodes["node-2"].store.get(
                "bounded"
            ) == "return"
        finally:
            release.set()


def test_higher_term_replication_response_steps_down_durably(
    tmp_path,
):
    with RaftCluster(data_dir=tmp_path) as cluster:
        leader = cluster.elect_leader("node-1")
        term = leader.state.current_term

        assert leader.put(
            "rejected",
            "value",
            HigherTermAppendTransport(),
        ) is False

        assert leader.state.role == NodeRole.FOLLOWER
        assert leader.state.current_term == term + 1
        assert leader.persistence is not None
        assert leader.persistence.load_state(
            leader.node_id
        ).current_term == term + 1


def test_higher_term_read_response_fails_authoritative_read():
    with RaftCluster() as cluster:
        leader = cluster.elect_leader("node-1")
        term = leader.state.current_term

        with pytest.raises(ReadQuorumError):
            leader.linearizable_get(
                "key",
                HigherTermAppendTransport(),
            )

        assert leader.state.role == NodeRole.FOLLOWER
        assert leader.state.current_term == term + 1


def test_higher_term_snapshot_response_steps_down_durably(
    tmp_path,
):
    with RaftCluster(data_dir=tmp_path) as cluster:
        leader = cluster.elect_leader("node-1")
        assert leader.put("a", "1", cluster.transport)
        assert leader.create_snapshot()
        assert leader.replication is not None

        for follower_id in leader.replication.followers:
            leader.replication.next_index[follower_id] = 1

        term = leader.state.current_term
        transport = HigherTermSnapshotTransport()

        for _ in range(3):
            if leader.state.role != NodeRole.LEADER:
                break

            leader.replicate_log(transport)

        assert transport.install_calls >= 1
        assert leader.state.role == NodeRole.FOLLOWER
        assert leader.state.current_term == term + 1
        assert leader.persistence is not None
        assert leader.persistence.load_state(
            leader.node_id
        ).current_term == term + 1


def test_follower_restart_recovers_and_catches_up(
    tmp_path,
):
    with RaftCluster(data_dir=tmp_path) as cluster:
        leader = cluster.elect_leader("node-1")
        assert leader.put(
            "before",
            "restart",
            cluster.transport,
        )

        cluster.transport.block("node-3")
        assert leader.put(
            "while-offline",
            "committed",
            cluster.transport,
        )

        restarted = cluster.restart_node("node-3")

        assert restarted.state.role == NodeRole.FOLLOWER
        assert restarted.store.get("before") == "restart"
        assert restarted.store.get("while-offline") is None

        cluster.transport.unblock("node-3")
        cluster.run_until(
            lambda: (
                restarted.store.get("while-offline")
                == "committed"
            )
        )

        assert restarted.state.last_applied == (
            restarted.state.commit_index
        )
        cluster.assert_converged()


def test_former_leader_restarts_as_follower(
    tmp_path,
):
    with RaftCluster(data_dir=tmp_path) as cluster:
        leader = cluster.elect_leader("node-1")
        assert leader.put(
            "durable",
            "value",
            cluster.transport,
        )

        restarted = cluster.restart_node("node-1")

        assert restarted.state.role == NodeRole.FOLLOWER
        assert restarted.state.leader_id is None
        assert restarted.store.get("durable") == "value"

        new_leader = cluster.elect_leader("node-2")
        assert new_leader.put(
            "after",
            "restart",
            cluster.transport,
        )

        cluster.run_until(
            lambda: (
                restarted.store.get("after") == "restart"
            )
        )
        cluster.assert_converged()


def test_healthy_leadership_transfer_preserves_data():
    with RaftCluster() as cluster:
        old_leader = cluster.elect_leader("node-1")
        assert old_leader.put(
            "before",
            "transfer",
            cluster.transport,
        )

        assert old_leader.transfer_leadership(
            cluster.transport,
            target_id="node-2",
        )
        assert old_leader.state.role == NodeRole.FOLLOWER

        new_leader = cluster.nodes["node-2"]
        new_leader.tick(cluster.transport)

        assert new_leader.state.role == NodeRole.LEADER
        assert new_leader.put(
            "after",
            "transfer",
            cluster.transport,
        )

        cluster.run_until(
            lambda: all(
                node.store.get("after") == "transfer"
                for node in cluster.nodes.values()
            )
        )
        cluster.assert_converged()


def test_failed_leadership_transfer_preserves_leader():
    with RaftCluster() as cluster:
        leader = cluster.elect_leader("node-1")
        assert leader.put(
            "before",
            "failure",
            cluster.transport,
        )
        cluster.transport.drop_timeout_now(
            "node-1",
            "node-2",
        )

        assert leader.transfer_leadership(
            cluster.transport,
            target_id="node-2",
        ) is False
        assert leader.state.role == NodeRole.LEADER
        assert leader.put(
            "after",
            "failure",
            cluster.transport,
        )
        assert leader.store.get("before") == "failure"
        assert leader.store.get("after") == "failure"
        cluster.assert_invariants()
