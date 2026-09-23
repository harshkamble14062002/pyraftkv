import subprocess
import sys
from pathlib import Path
from unittest.mock import patch
from urllib.parse import unquote

import pytest

from scripts.demo_failover import (
    DemoError,
    DockerCompose,
    FailoverDemo,
    Leader,
    lagging_nodes,
    select_leader,
)


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, duration: float) -> None:
        self.now += duration


class FakeDockerCluster:
    def __init__(self) -> None:
        self.running = {
            "node1",
            "node2",
            "node3",
        }
        self.leader = "node2"
        self.term = 4
        self.commit_indexes = {
            node_id: 0
            for node_id in self.running
        }
        self.values: dict[str, str] = {}
        self.stop_calls: list[str] = []
        self.start_calls: list[str] = []
        self.up_build: bool | None = None
        self.verified = False

    def verify(self) -> None:
        self.verified = True

    def up(self, build: bool) -> None:
        self.up_build = build

    def stop(self, node_id: str) -> None:
        self.stop_calls.append(node_id)
        self.running.remove(node_id)
        self.leader = min(self.running)
        self.term += 1

    def start(self, node_id: str) -> None:
        self.start_calls.append(node_id)
        self.running.add(node_id)
        self.commit_indexes[node_id] = max(
            self.commit_indexes.values()
        )

    def diagnostics(self) -> str:
        return "fake diagnostics"

    def request_json(
        self,
        node_id: str,
        path: str,
        *,
        method: str = "GET",
        payload=None,
    ):
        if node_id not in self.running:
            raise DemoError(f"{node_id} is stopped")

        if path == "/health":
            return {
                "status": "ok",
            }

        if path == "/cluster/status":
            index = self.commit_indexes[node_id]
            return {
                "node_id": node_id,
                "role": (
                    "leader"
                    if node_id == self.leader
                    else "follower"
                ),
                "current_term": self.term,
                "commit_index": index,
                "last_applied": index,
                "log_last_index": index,
            }

        if path.startswith("/kv/"):
            key = unquote(path.removeprefix("/kv/"))

            if node_id != self.leader:
                raise DemoError("not leader")

            if method == "PUT":
                assert payload is not None
                value = payload["value"]
                self.values[key] = value
                next_index = max(
                    self.commit_indexes.values()
                ) + 1

                for running_node in self.running:
                    self.commit_indexes[
                        running_node
                    ] = next_index

                return {
                    "committed": True,
                    "leader_id": node_id,
                    "key": key,
                    "value": value,
                }

            return {
                "key": key,
                "value": self.values[key],
                "leader_id": node_id,
            }

        raise AssertionError(f"unexpected path: {path}")

    def request_text(
        self,
        node_id: str,
        path: str,
    ) -> str:
        assert node_id in self.running
        assert path == "/metrics"
        return "pyraftkv_raft_current_term 5\n"


def status(
    role: str,
    term: int,
    index: int = 0,
):
    return {
        "role": role,
        "current_term": term,
        "commit_index": index,
        "last_applied": index,
        "log_last_index": index,
    }


def test_select_leader_requires_exactly_one_valid_leader():
    statuses = {
        "node1": status("follower", 3),
        "node2": status("leader", 3),
        "node3": status("follower", 3),
    }

    assert select_leader(statuses) == Leader(
        node_id="node2",
        term=3,
    )
    assert select_leader(
        statuses,
        excluded_node="node2",
    ) is None

    statuses["node3"] = status("leader", 4)

    assert select_leader(statuses) is None


def test_lagging_nodes_checks_commit_apply_and_log_indexes():
    statuses = {
        "node1": status("leader", 4, index=8),
        "node2": status("follower", 4, index=8),
        "node3": {
            **status("follower", 4, index=8),
            "last_applied": 7,
        },
    }

    assert lagging_nodes(statuses, 8) == ["node3"]

    del statuses["node2"]

    assert lagging_nodes(statuses, 8) == [
        "node2",
        "node3",
    ]


def test_demo_discovers_leader_and_completes_failover():
    cluster = FakeDockerCluster()
    clock = FakeClock()
    demo = FailoverDemo(
        cluster,
        cluster,
        timeout=10,
        poll_interval=0.1,
        stable_samples=2,
        clock=clock,
        sleeper=clock.sleep,
    )

    demo.run(build=False)

    assert cluster.verified is True
    assert cluster.up_build is False
    assert cluster.stop_calls == ["node2"]
    assert cluster.start_calls == ["node2"]
    assert cluster.running == {
        "node1",
        "node2",
        "node3",
    }
    assert len(set(cluster.commit_indexes.values())) == 1


def test_stable_leader_wait_is_bounded():
    class LeaderlessCluster(FakeDockerCluster):
        def request_json(self, node_id, path, **kwargs):
            response = super().request_json(
                node_id,
                path,
                **kwargs,
            )

            if path == "/cluster/status":
                response["role"] = "follower"

            return response

    cluster = LeaderlessCluster()
    clock = FakeClock()
    demo = FailoverDemo(
        cluster,
        cluster,
        timeout=0.3,
        poll_interval=0.1,
        stable_samples=2,
        clock=clock,
        sleeper=clock.sleep,
    )

    with pytest.raises(
        DemoError,
        match="leader did not stabilize",
    ):
        demo.wait_for_stable_leader()


def test_missing_docker_reports_actionable_error():
    compose = DockerCompose()

    with patch(
        "scripts.demo_failover.shutil.which",
        return_value=None,
    ), pytest.raises(DemoError, match="not installed"):
        compose.verify()


def test_demo_cli_help_runs_without_docker():
    project_root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [
            sys.executable,
            "scripts/demo_failover.py",
            "--help",
        ],
        cwd=project_root,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    normalized_help = " ".join(result.stdout.split())

    assert (
        "without assuming which node is leader"
        in normalized_help
    )
    assert "--no-build" in normalized_help
