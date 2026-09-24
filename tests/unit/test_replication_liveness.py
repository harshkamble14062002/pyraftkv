from concurrent.futures import ThreadPoolExecutor
from time import perf_counter

from tests.helpers.raft_cluster import RaftCluster


def test_replication_returns_after_first_healthy_follower():
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


def test_stale_in_flight_rpc_catches_up_before_success():
    with RaftCluster() as cluster:
        leader = cluster.elect_leader("node-1")
        release = cluster.transport.delay_append_entries(
            "node-1",
            "node-2",
        )

        try:
            assert leader.put(
                "first",
                "committed",
                cluster.transport,
            )

            cluster.transport.block("node-3")

            with ThreadPoolExecutor(
                max_workers=1,
            ) as executor:
                future = executor.submit(
                    leader.put,
                    "second",
                    "committed",
                    cluster.transport,
                )

                assert not future.done()
                release.set()

                assert future.result(timeout=0.5)

            assert cluster.nodes["node-2"].store.get(
                "second"
            ) == "committed"
        finally:
            release.set()
