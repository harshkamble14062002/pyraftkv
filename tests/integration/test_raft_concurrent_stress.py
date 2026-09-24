import random
from collections.abc import Callable
from concurrent.futures import (
    Future,
    ThreadPoolExecutor,
    wait,
)
from functools import partial

import pytest

from pyraftkv.raft.node import (
    NotLeaderError,
    ReadQuorumError,
)
from pyraftkv.raft.state import NodeRole
from tests.helpers.raft_cluster import RaftCluster


def run_concurrently(
    cluster: RaftCluster,
    operations: list[Callable[[], object]],
    max_workers: int = 8,
    timeout: float = 3.0,
) -> list[object]:
    executor = ThreadPoolExecutor(
        max_workers=max_workers,
    )
    futures: list[Future[object]] = []

    try:
        futures = [
            executor.submit(operation)
            for operation in operations
        ]
        _, pending = wait(
            futures,
            timeout=timeout,
        )

        if pending:
            cluster.heal()

            for future in pending:
                future.cancel()

            raise AssertionError(
                f"{len(pending)} concurrent operations "
                f"did not finish; {cluster.diagnostics()}"
            )

        return [
            future.result()
            for future in futures
        ]
    finally:
        executor.shutdown(
            wait=True,
            cancel_futures=True,
        )


def all_nodes_contain(
    cluster: RaftCluster,
    expected: dict[str, str],
) -> bool:
    return all(
        all(
            node.store.get(key) == value
            for key, value in expected.items()
        )
        for node in cluster.nodes.values()
    )


def test_concurrent_unique_key_puts_converge():
    with RaftCluster() as cluster:
        leader = cluster.elect_leader("node-1")
        expected = {
            f"worker-{worker}-key-{index}": (
                f"value-{worker}-{index}"
            )
            for worker in range(6)
            for index in range(8)
        }
        operations = [
            partial(
                leader.put,
                key,
                value,
                cluster.transport,
            )
            for key, value in expected.items()
        ]

        results = run_concurrently(
            cluster,
            operations,
        )

        assert all(result is True for result in results)

        cluster.run_until(
            lambda: all_nodes_contain(
                cluster,
                expected,
            )
        )
        cluster.assert_converged()

        indexes = [
            entry.index
            for entry in leader.log.entries_from(
                leader.log.first_index
            )
        ]
        assert indexes == list(
            range(1, len(expected) + 1)
        )


def test_deterministic_mixed_workload_converges():
    seed = 4601
    generator = random.Random(seed)

    with RaftCluster() as cluster:
        leader = cluster.elect_leader("node-1")
        expected: dict[str, str] = {}

        for index in range(10):
            read_key = f"read-{index}"
            delete_key = f"delete-{index}"
            assert leader.put(
                read_key,
                f"stable-{index}",
                cluster.transport,
            )
            assert leader.put(
                delete_key,
                f"remove-{index}",
                cluster.transport,
            )
            expected[read_key] = f"stable-{index}"

        workload: list[
            tuple[str, str, str | None, Callable[[], object]]
        ] = []

        for index in range(20):
            key = f"put-{index}"
            value = f"value-{index}"
            expected[key] = value
            workload.append(
                (
                    "put",
                    key,
                    value,
                    partial(
                        leader.put,
                        key,
                        value,
                        cluster.transport,
                    ),
                )
            )

        for index in range(10):
            key = f"read-{index}"
            workload.append(
                (
                    "get",
                    key,
                    f"stable-{index}",
                    partial(
                        leader.linearizable_get,
                        key,
                        cluster.transport,
                    ),
                )
            )

        for index in range(10):
            key = f"delete-{index}"
            workload.append(
                (
                    "delete",
                    key,
                    None,
                    partial(
                        leader.delete,
                        key,
                        cluster.transport,
                    ),
                )
            )

        generator.shuffle(workload)
        results = run_concurrently(
            cluster,
            [
                operation
                for _, _, _, operation in workload
            ],
        )

        for (
            operation,
            key,
            expected_result,
            _,
        ), result in zip(workload, results, strict=True):
            if operation == "get":
                assert result == expected_result, (
                    f"seed={seed}, key={key}"
                )
            else:
                assert result is True, (
                    f"seed={seed}, operation={operation}, "
                    f"key={key}"
                )

        cluster.run_until(
            lambda: all(
                node.store.snapshot() == expected
                for node in cluster.nodes.values()
            )
        )
        cluster.assert_converged()


def test_concurrent_traffic_continues_during_follower_failure():
    with RaftCluster() as cluster:
        leader = cluster.elect_leader("node-1")
        cluster.transport.block("node-3")
        expected = {
            f"offline-{index}": f"value-{index}"
            for index in range(36)
        }

        results = run_concurrently(
            cluster,
            [
                partial(
                    leader.put,
                    key,
                    value,
                    cluster.transport,
                )
                for key, value in expected.items()
            ],
        )

        assert all(result is True for result in results)
        assert leader.state.role == NodeRole.LEADER

        cluster.transport.unblock("node-3")
        after_rejoin = {
            f"rejoined-{index}": f"value-{index}"
            for index in range(12)
        }
        expected.update(after_rejoin)

        results = run_concurrently(
            cluster,
            [
                partial(
                    leader.put,
                    key,
                    value,
                    cluster.transport,
                )
                for key, value in after_rejoin.items()
            ],
        )

        assert all(result is True for result in results)

        cluster.run_until(
            lambda: all_nodes_contain(
                cluster,
                expected,
            )
        )
        cluster.assert_converged()


def test_concurrent_traffic_survives_leader_loss():
    with RaftCluster() as cluster:
        old_leader = cluster.elect_leader("node-1")
        before = {
            f"before-{index}": f"value-{index}"
            for index in range(18)
        }

        before_results = run_concurrently(
            cluster,
            [
                partial(
                    old_leader.put,
                    key,
                    value,
                    cluster.transport,
                )
                for key, value in before.items()
            ],
        )
        assert all(
            result is True
            for result in before_results
        )

        cluster.isolate("node-1")

        with pytest.raises(ReadQuorumError):
            old_leader.linearizable_get(
                "before-0",
                cluster.transport,
            )

        indeterminate = {
            f"during-failover-{index}": f"value-{index}"
            for index in range(8)
        }
        isolated_results = run_concurrently(
            cluster,
            [
                partial(
                    old_leader.put,
                    key,
                    value,
                    cluster.transport,
                )
                for key, value in indeterminate.items()
            ],
        )
        assert all(
            result is False
            for result in isolated_results
        )

        candidate_id = max(
            ("node-2", "node-3"),
            key=lambda node_id: (
                cluster.nodes[node_id].log.last_term,
                cluster.nodes[node_id].log.last_index,
            ),
        )
        new_leader = cluster.elect_leader(
            candidate_id
        )
        after = {
            f"after-{index}": f"value-{index}"
            for index in range(18)
        }
        after_results = run_concurrently(
            cluster,
            [
                partial(
                    new_leader.put,
                    key,
                    value,
                    cluster.transport,
                )
                for key, value in after.items()
            ],
        )
        assert all(
            result is True
            for result in after_results
        )

        acknowledged = before | after
        cluster.heal()
        cluster.run_until(
            lambda: (
                all_nodes_contain(
                    cluster,
                    acknowledged,
                )
                and all(
                    node.state.commit_index
                    == new_leader.state.commit_index
                    for node in cluster.nodes.values()
                )
            )
        )

        assert old_leader.state.role == NodeRole.FOLLOWER

        with pytest.raises(NotLeaderError):
            old_leader.linearizable_get(
                "before-0",
                cluster.transport,
            )

        cluster.assert_converged()


def test_concurrent_traffic_during_snapshot_catch_up():
    with RaftCluster() as cluster:
        leader = cluster.elect_leader("node-1")
        cluster.transport.block("node-3")
        before_snapshot = {
            f"snapshot-{index}": f"value-{index}"
            for index in range(24)
        }

        results = run_concurrently(
            cluster,
            [
                partial(
                    leader.put,
                    key,
                    value,
                    cluster.transport,
                )
                for key, value in before_snapshot.items()
            ],
        )
        assert all(result is True for result in results)
        assert leader.create_snapshot()

        cluster.transport.unblock("node-3")
        suffix = {
            f"suffix-{index}": f"value-{index}"
            for index in range(16)
        }
        results = run_concurrently(
            cluster,
            [
                partial(
                    leader.put,
                    key,
                    value,
                    cluster.transport,
                )
                for key, value in suffix.items()
            ],
        )
        assert all(result is True for result in results)

        expected = before_snapshot | suffix
        cluster.run_until(
            lambda: (
                all_nodes_contain(
                    cluster,
                    expected,
                )
                and all(
                    node.state.commit_index
                    == leader.state.commit_index
                    for node in cluster.nodes.values()
                )
            )
        )

        assert cluster.transport.call_count(
            "install_snapshot",
            "node-1",
            "node-3",
        ) >= 1
        assert cluster.nodes["node-3"].log.base_index == (
            leader.log.base_index
        )
        cluster.assert_converged()
