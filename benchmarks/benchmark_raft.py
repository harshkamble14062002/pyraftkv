import argparse
import json
import random
from collections.abc import Callable
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
)
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from time import perf_counter
from typing import Any

from benchmarks.benchmark_http import summarize
from pyraftkv.raft.node import RaftNode
from pyraftkv.raft.state import NodeRole
from pyraftkv.transport.memory import InMemoryTransport

DISCLAIMER = (
    "These are local development-machine measurements and are not "
    "representative of production cluster performance."
)
SUMMARY_FIELDS = (
    "requests",
    "successes",
    "failures",
    "throughput_rps",
    "latency_mean_ms",
    "latency_p50_ms",
    "latency_p95_ms",
    "latency_p99_ms",
)


class LocalRaftCluster:
    def __init__(self) -> None:
        self.members = {
            "node-1",
            "node-2",
            "node-3",
        }
        self.transport = InMemoryTransport()
        self.nodes = {
            node_id: RaftNode(
                node_id,
                self.members,
            )
            for node_id in self.members
        }

        for node_id, node in self.nodes.items():
            self.transport.register(
                node_id,
                node,
            )

        self.leader = self.elect("node-1")

    def elect(self, node_id: str) -> RaftNode:
        node = self.nodes[node_id]
        node.timer.expire_now()
        node.tick(self.transport)

        if node.state.role != NodeRole.LEADER:
            raise RuntimeError(
                f"{node_id} did not become leader"
            )

        return node

    def replicate_until(
        self,
        predicate: Callable[[], bool],
        max_rounds: int = 100,
    ) -> bool:
        for _ in range(max_rounds + 1):
            if predicate():
                return True

            for node in self.nodes.values():
                if node.state.role == NodeRole.LEADER:
                    node.replicate_log(
                        self.transport
                    )

        return False

    def close(self) -> None:
        self.transport.heal()

        for node in self.nodes.values():
            node.close(wait_for_workers=True)


def timed_operation(
    operation: Callable[[], bool],
) -> tuple[bool, float]:
    started = perf_counter()

    try:
        success = operation()
    except Exception:  # noqa: BLE001 - benchmark records failed requests
        success = False

    return success, perf_counter() - started


def run_operations(
    operations: list[Callable[[], bool]],
    concurrency: int,
) -> dict[str, float | int]:
    latencies: list[float] = []
    successes = 0
    failures = 0
    started = perf_counter()

    with ThreadPoolExecutor(
        max_workers=concurrency,
    ) as executor:
        futures = [
            executor.submit(
                timed_operation,
                operation,
            )
            for operation in operations
        ]

        for future in as_completed(futures):
            success, latency = future.result()
            latencies.append(latency)

            if success:
                successes += 1
            else:
                failures += 1

    duration = perf_counter() - started

    return summarize(
        latencies,
        successes,
        failures,
        duration,
    )


def workload_result(
    scenario: str,
    operation: str,
    requests: int,
    concurrency: int,
    unavailable_follower: bool = False,
) -> dict[str, object]:
    cluster = LocalRaftCluster()

    try:
        leader = cluster.leader

        if unavailable_follower:
            cluster.transport.block("node-3")

        if operation == "put":
            operations = [
                partial(
                    leader.put,
                    f"benchmark-{index}",
                    f"value-{index}",
                    cluster.transport,
                )
                for index in range(requests)
            ]
        elif operation == "get":
            if not leader.put(
                "benchmark-read",
                "value",
                cluster.transport,
            ):
                raise RuntimeError(
                    "Unable to prepare GET benchmark"
                )

            operations = [
                lambda: (
                    leader.linearizable_get(
                        "benchmark-read",
                        cluster.transport,
                    )
                    == "value"
                )
                for _ in range(requests)
            ]
        elif operation == "mixed":
            generator = random.Random(46)
            operations = []

            for index in range(requests):
                choice = generator.randrange(3)
                key = f"mixed-{index}"

                if choice == 0:
                    operations.append(
                        partial(
                            leader.put,
                            key,
                            "value",
                            cluster.transport,
                        )
                    )
                elif choice == 1:
                    operations.append(
                        lambda: (
                            leader.linearizable_get(
                                "missing",
                                cluster.transport,
                            )
                            is None
                        )
                    )
                else:
                    operations.append(
                        partial(
                            leader.delete,
                            key,
                            cluster.transport,
                        )
                    )
        else:
            raise ValueError(
                f"Unsupported operation: {operation}"
            )

        return {
            "scenario": scenario,
            "operation": operation,
            "concurrency": concurrency,
            **run_operations(
                operations,
                concurrency,
            ),
        }
    finally:
        cluster.close()


def recovery_summary(
    scenario: str,
    duration: float,
    success: bool,
    **details: object,
) -> dict[str, object]:
    return {
        "scenario": scenario,
        **details,
        **summarize(
            [duration],
            successes=1 if success else 0,
            failures=0 if success else 1,
            duration=duration,
        ),
    }


def benchmark_leader_failover() -> dict[str, object]:
    cluster = LocalRaftCluster()

    try:
        leader = cluster.leader

        if not leader.put(
            "durable",
            "before-failover",
            cluster.transport,
        ):
            raise RuntimeError(
                "Unable to prepare failover benchmark"
            )

        cluster.transport.block(leader.node_id)
        started = perf_counter()
        survivors = [
            node_id
            for node_id in cluster.members
            if node_id != leader.node_id
        ]
        candidate_id = max(
            survivors,
            key=lambda node_id: (
                cluster.nodes[node_id].log.last_term,
                cluster.nodes[node_id].log.last_index,
            ),
        )
        new_leader = cluster.elect(candidate_id)
        duration = perf_counter() - started
        success = (
            new_leader.state.role == NodeRole.LEADER
            and new_leader.store.get("durable")
            == "before-failover"
        )

        return recovery_summary(
            "leader_failover",
            duration,
            success,
            old_leader=leader.node_id,
            new_leader=new_leader.node_id,
        )
    finally:
        cluster.close()


def benchmark_follower_catch_up(
    entries: int,
    snapshot: bool,
) -> dict[str, object]:
    cluster = LocalRaftCluster()

    try:
        leader = cluster.leader
        follower = cluster.nodes["node-3"]
        cluster.transport.block(follower.node_id)

        for index in range(entries):
            if not leader.put(
                f"catch-up-{index}",
                f"value-{index}",
                cluster.transport,
            ):
                raise RuntimeError(
                    "Unable to prepare catch-up benchmark"
                )

        if snapshot:
            if not leader.create_snapshot():
                raise RuntimeError(
                    "Unable to create benchmark snapshot"
                )

            if not leader.put(
                "retained-suffix",
                "value",
                cluster.transport,
            ):
                raise RuntimeError(
                    "Unable to append retained suffix"
                )

        cluster.transport.unblock(follower.node_id)
        started = perf_counter()
        success = cluster.replicate_until(
            lambda: (
                follower.store.snapshot()
                == leader.store.snapshot()
                and follower.state.commit_index
                == leader.state.commit_index
            )
        )
        duration = perf_counter() - started

        return recovery_summary(
            (
                "snapshot_catch_up"
                if snapshot
                else "follower_catch_up"
            ),
            duration,
            success,
            entries=entries,
            install_snapshot_calls=(
                cluster.transport.call_count(
                    "install_snapshot",
                    leader.node_id,
                    follower.node_id,
                )
            ),
        )
    finally:
        cluster.close()


def run_suite(
    requests: int,
    concurrency: int,
    recovery_entries: int,
) -> dict[str, Any]:
    if requests <= 0:
        raise ValueError("requests must be positive")

    if concurrency <= 0:
        raise ValueError(
            "concurrency must be positive"
        )

    if recovery_entries <= 0:
        raise ValueError(
            "recovery_entries must be positive"
        )

    return {
        "format_version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "environment": {
            "transport": "in_memory",
            "cluster_size": 3,
            "python_benchmark": True,
        },
        "disclaimer": DISCLAIMER,
        "configuration": {
            "requests": requests,
            "concurrency": concurrency,
            "recovery_entries": recovery_entries,
        },
        "scenarios": {
            "normal_put": workload_result(
                "normal_put",
                "put",
                requests,
                concurrency,
            ),
            "linearizable_get": workload_result(
                "linearizable_get",
                "get",
                requests,
                concurrency,
            ),
            "follower_unavailable": workload_result(
                "follower_unavailable",
                "put",
                requests,
                concurrency,
                unavailable_follower=True,
            ),
            "mixed_workload": workload_result(
                "mixed_workload",
                "mixed",
                requests,
                concurrency,
            ),
            "leader_failover": (
                benchmark_leader_failover()
            ),
            "follower_catch_up": (
                benchmark_follower_catch_up(
                    recovery_entries,
                    snapshot=False,
                )
            ),
            "snapshot_catch_up": (
                benchmark_follower_catch_up(
                    recovery_entries,
                    snapshot=True,
                )
            ),
        },
    }


def markdown_summary(
    result: dict[str, Any],
) -> str:
    lines = [
        "# PyRaftKV local benchmark summary",
        "",
        f"> {result['disclaimer']}",
        "",
        (
            "| Scenario | Requests | Successes | Failures | "
            "Throughput (req/s) | Mean (ms) | p50 | p95 | p99 |"
        ),
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]

    for name, scenario in result["scenarios"].items():
        lines.append(
            f"| {name} | {scenario['requests']} | "
            f"{scenario['successes']} | "
            f"{scenario['failures']} | "
            f"{scenario['throughput_rps']} | "
            f"{scenario['latency_mean_ms']} | "
            f"{scenario['latency_p50_ms']} | "
            f"{scenario['latency_p95_ms']} | "
            f"{scenario['latency_p99_ms']} |"
        )

    return "\n".join(lines) + "\n"


def write_results(
    result: dict[str, Any],
    output: Path,
    markdown: Path | None = None,
) -> None:
    output.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    output.write_text(
        json.dumps(
            result,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    if markdown is not None:
        markdown.parent.mkdir(
            parents=True,
            exist_ok=True,
        )
        markdown.write_text(
            markdown_summary(result),
            encoding="utf-8",
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark local three-node PyRaftKV "
            "workloads and recovery"
        )
    )
    parser.add_argument(
        "--requests",
        type=int,
        default=1000,
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
    )
    parser.add_argument(
        "--recovery-entries",
        type=int,
        default=100,
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(
            "benchmarks/results/"
            "local-raft-latest.json"
        ),
    )
    parser.add_argument(
        "--markdown",
        type=Path,
        default=Path(
            "benchmarks/results/"
            "local-raft-latest.md"
        ),
    )
    args = parser.parse_args()
    result = run_suite(
        requests=args.requests,
        concurrency=args.concurrency,
        recovery_entries=args.recovery_entries,
    )
    write_results(
        result,
        args.output,
        args.markdown,
    )
    print(
        json.dumps(
            result,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
