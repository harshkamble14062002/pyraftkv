#!/usr/bin/env python3
"""Run a reproducible three-node Docker failover demonstration."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class NodeEndpoint:
    node_id: str
    base_url: str


@dataclass(frozen=True)
class Leader:
    node_id: str
    term: int


NODES = (
    NodeEndpoint("node1", "http://127.0.0.1:8001"),
    NodeEndpoint("node2", "http://127.0.0.1:8002"),
    NodeEndpoint("node3", "http://127.0.0.1:8003"),
)
NODE_IDS = tuple(node.node_id for node in NODES)
NODE_BY_ID = {node.node_id: node for node in NODES}


class DemoError(RuntimeError):
    """Raised when a bounded demo step cannot be completed."""


class DockerCompose:
    def __init__(
        self,
        project_root: Path = PROJECT_ROOT,
        command_timeout: float = 300.0,
    ) -> None:
        if command_timeout <= 0:
            raise ValueError(
                "command_timeout must be positive"
            )

        self.project_root = project_root
        self.command_timeout = command_timeout

    def run(
        self,
        *arguments: str,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        command = [
            "docker",
            "compose",
            *arguments,
        ]

        try:
            result = subprocess.run(
                command,
                cwd=self.project_root,
                text=True,
                capture_output=True,
                check=False,
                timeout=self.command_timeout,
            )
        except subprocess.TimeoutExpired as exc:
            raise DemoError(
                f"{' '.join(command)} exceeded "
                f"{self.command_timeout:g}s"
            ) from exc
        except OSError as exc:
            raise DemoError(
                f"unable to run {' '.join(command)}: {exc}"
            ) from exc

        if check and result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip()
            raise DemoError(
                f"{' '.join(command)} failed: {detail}"
            )

        return result

    def verify(self) -> None:
        if shutil.which("docker") is None:
            raise DemoError("docker is not installed or not on PATH")

        self.run("version")
        self.run("config", "--quiet")

    def up(self, build: bool) -> None:
        arguments = ["up", "-d"]

        if build:
            arguments.append("--build")

        arguments.extend(NODE_IDS)
        self.run(*arguments)

    def stop(self, node_id: str) -> None:
        self.run("stop", node_id)

    def start(self, node_id: str) -> None:
        self.run("start", node_id)

    def diagnostics(self) -> str:
        sections: list[str] = []

        for title, arguments in (
            ("docker compose ps", ("ps",)),
            (
                "recent node logs",
                (
                    "logs",
                    "--tail",
                    "80",
                    *NODE_IDS,
                ),
            ),
        ):
            try:
                result = self.run(
                    *arguments,
                    check=False,
                )
            except DemoError as exc:
                sections.append(
                    f"--- {title} ---\n{exc}"
                )
                continue

            output = result.stdout.strip()
            error = result.stderr.strip()
            sections.append(
                "\n".join(
                    part
                    for part in (
                        f"--- {title} ---",
                        output,
                        error,
                    )
                    if part
                )
            )

        return "\n".join(sections)


class ClusterHTTP:
    def __init__(self, request_timeout: float = 2.0) -> None:
        self.request_timeout = request_timeout

    def request_json(
        self,
        node_id: str,
        path: str,
        *,
        method: str = "GET",
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        endpoint = NODE_BY_ID[node_id]
        body = None
        headers: dict[str, str] = {}

        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        request = Request(
            f"{endpoint.base_url}{path}",
            data=body,
            headers=headers,
            method=method,
        )

        try:
            with urlopen(
                request,
                timeout=self.request_timeout,
            ) as response:
                raw_body = response.read()
        except HTTPError as exc:
            detail = exc.read().decode(
                "utf-8",
                errors="replace",
            )
            raise DemoError(
                f"{method} {request.full_url} returned "
                f"HTTP {exc.code}: {detail}"
            ) from exc
        except (TimeoutError, URLError) as exc:
            raise DemoError(
                f"{method} {request.full_url} failed: {exc}"
            ) from exc

        try:
            decoded = json.loads(raw_body)
        except (TypeError, json.JSONDecodeError) as exc:
            raise DemoError(
                f"{method} {request.full_url} returned invalid JSON"
            ) from exc

        if not isinstance(decoded, dict):
            raise DemoError(
                f"{method} {request.full_url} returned a non-object"
            )

        return decoded

    def request_text(
        self,
        node_id: str,
        path: str,
    ) -> str:
        endpoint = NODE_BY_ID[node_id]
        request = Request(f"{endpoint.base_url}{path}")

        try:
            with urlopen(
                request,
                timeout=self.request_timeout,
            ) as response:
                return response.read().decode("utf-8")
        except (HTTPError, TimeoutError, URLError) as exc:
            raise DemoError(
                f"GET {request.full_url} failed: {exc}"
            ) from exc


def select_leader(
    statuses: dict[str, dict[str, Any]],
    excluded_node: str | None = None,
) -> Leader | None:
    leaders: list[Leader] = []

    for node_id, status in statuses.items():
        if node_id == excluded_node:
            continue

        if status.get("role") != "leader":
            continue

        term = status.get("current_term")

        if not isinstance(term, int):
            continue

        leaders.append(
            Leader(
                node_id=node_id,
                term=term,
            )
        )

    if len(leaders) != 1:
        return None

    return leaders[0]


def lagging_nodes(
    statuses: dict[str, dict[str, Any]],
    target_commit_index: int,
) -> list[str]:
    lagging: list[str] = []

    for node_id in NODE_IDS:
        status = statuses.get(node_id)

        if status is None:
            lagging.append(node_id)
            continue

        indexes = (
            status.get("commit_index"),
            status.get("last_applied"),
            status.get("log_last_index"),
        )

        if any(
            not isinstance(index, int)
            or index < target_commit_index
            for index in indexes
        ):
            lagging.append(node_id)

    return lagging


class FailoverDemo:
    def __init__(
        self,
        compose: DockerCompose,
        http: ClusterHTTP,
        *,
        timeout: float,
        poll_interval: float,
        stable_samples: int,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if timeout <= 0:
            raise ValueError("timeout must be positive")

        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")

        if stable_samples <= 0:
            raise ValueError("stable_samples must be positive")

        self.compose = compose
        self.http = http
        self.timeout = timeout
        self.poll_interval = poll_interval
        self.stable_samples = stable_samples
        self.clock = clock
        self.sleeper = sleeper
        self.stopped_node: str | None = None

    def log(self, message: str) -> None:
        print(f"[demo] {message}", flush=True)

    def collect_statuses(
        self,
    ) -> dict[str, dict[str, Any]]:
        statuses: dict[str, dict[str, Any]] = {}

        for node_id in NODE_IDS:
            try:
                statuses[node_id] = self.http.request_json(
                    node_id,
                    "/cluster/status",
                )
            except DemoError:
                continue

        return statuses

    def wait_for_health(
        self,
        required_nodes: tuple[str, ...] = NODE_IDS,
    ) -> None:
        deadline = self.clock() + self.timeout
        unhealthy = list(required_nodes)

        while self.clock() < deadline:
            unhealthy = []

            for node_id in required_nodes:
                try:
                    health = self.http.request_json(
                        node_id,
                        "/health",
                    )
                except DemoError:
                    unhealthy.append(node_id)
                    continue

                if health.get("status") != "ok":
                    unhealthy.append(node_id)

            if not unhealthy:
                return

            self.sleeper(self.poll_interval)

        raise DemoError(
            "health timeout; unavailable nodes: "
            + ", ".join(unhealthy)
        )

    def wait_for_stable_leader(
        self,
        excluded_node: str | None = None,
    ) -> Leader:
        deadline = self.clock() + self.timeout
        previous: Leader | None = None
        consecutive = 0
        last_statuses: dict[str, dict[str, Any]] = {}

        while self.clock() < deadline:
            last_statuses = self.collect_statuses()
            candidate = select_leader(
                last_statuses,
                excluded_node,
            )

            if candidate is not None and candidate == previous:
                consecutive += 1
            elif candidate is not None:
                previous = candidate
                consecutive = 1
            else:
                previous = None
                consecutive = 0

            if (
                candidate is not None
                and consecutive >= self.stable_samples
            ):
                return candidate

            self.sleeper(self.poll_interval)

        raise DemoError(
            "leader did not stabilize; last statuses: "
            + json.dumps(last_statuses, sort_keys=True)
        )

    def put(
        self,
        leader: Leader,
        key: str,
        value: str,
    ) -> None:
        response = self.http.request_json(
            leader.node_id,
            f"/kv/{quote(key, safe='')}",
            method="PUT",
            payload={"value": value},
        )

        if (
            response.get("committed") is not True
            or response.get("leader_id") != leader.node_id
        ):
            raise DemoError(
                "write was not confirmed by the expected leader: "
                + json.dumps(response, sort_keys=True)
            )

    def verify_read(
        self,
        leader: Leader,
        key: str,
        expected_value: str,
    ) -> None:
        response = self.http.request_json(
            leader.node_id,
            f"/kv/{quote(key, safe='')}",
        )

        if response.get("value") != expected_value:
            raise DemoError(
                f"linearizable read returned {response!r}; "
                f"expected value {expected_value!r}"
            )

    def wait_for_catch_up(
        self,
        target_commit_index: int,
    ) -> None:
        deadline = self.clock() + self.timeout
        statuses: dict[str, dict[str, Any]] = {}
        lagging = list(NODE_IDS)

        while self.clock() < deadline:
            statuses = self.collect_statuses()
            lagging = lagging_nodes(
                statuses,
                target_commit_index,
            )

            if not lagging:
                return

            self.sleeper(self.poll_interval)

        raise DemoError(
            f"catch-up timeout at index {target_commit_index}; "
            f"lagging={lagging}; statuses="
            + json.dumps(statuses, sort_keys=True)
        )

    def verify_metrics(self) -> None:
        for node_id in NODE_IDS:
            metrics = self.http.request_text(
                node_id,
                "/metrics",
            )

            if "pyraftkv_raft_current_term" not in metrics:
                raise DemoError(
                    f"{node_id} metrics are missing Raft gauges"
                )

    def diagnostics(self) -> None:
        print(
            "\n[demo] failure diagnostics",
            file=sys.stderr,
        )
        print(
            self.compose.diagnostics(),
            file=sys.stderr,
        )
        print(
            "--- reachable cluster status ---",
            file=sys.stderr,
        )
        print(
            json.dumps(
                self.collect_statuses(),
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )

    def run(self, *, build: bool) -> None:
        self.log("validating Docker Compose configuration")
        self.compose.verify()

        self.log("starting the three-node cluster")
        self.compose.up(build=build)
        self.wait_for_health()

        initial_leader = self.wait_for_stable_leader()
        self.log(
            f"initial leader={initial_leader.node_id} "
            f"term={initial_leader.term}"
        )

        unique = f"{int(time.time())}-{os.getpid()}"
        before_key = f"demo-before-{unique}"
        after_key = f"demo-after-{unique}"

        self.log("committing and reading a value before failure")
        self.put(
            initial_leader,
            before_key,
            "before-failover",
        )
        self.verify_read(
            initial_leader,
            before_key,
            "before-failover",
        )

        self.log(
            f"stopping dynamically discovered leader "
            f"{initial_leader.node_id}"
        )
        self.compose.stop(initial_leader.node_id)
        self.stopped_node = initial_leader.node_id

        new_leader = self.wait_for_stable_leader(
            excluded_node=initial_leader.node_id,
        )

        if new_leader.term <= initial_leader.term:
            raise DemoError(
                f"new leader term {new_leader.term} did not advance "
                f"past {initial_leader.term}"
            )

        self.log(
            f"new leader={new_leader.node_id} "
            f"term={new_leader.term}"
        )

        self.log("committing and reading with two of three nodes")
        self.put(
            new_leader,
            after_key,
            "after-failover",
        )
        self.verify_read(
            new_leader,
            after_key,
            "after-failover",
        )
        self.verify_read(
            new_leader,
            before_key,
            "before-failover",
        )

        leader_status = self.http.request_json(
            new_leader.node_id,
            "/cluster/status",
        )
        target_commit_index = leader_status.get(
            "commit_index"
        )

        if not isinstance(target_commit_index, int):
            raise DemoError(
                "leader status did not contain a commit index"
            )

        self.log(
            f"restarting {initial_leader.node_id} and waiting "
            f"for commit index {target_commit_index}"
        )
        self.compose.start(initial_leader.node_id)
        self.stopped_node = None
        self.wait_for_health((initial_leader.node_id,))
        self.wait_for_catch_up(target_commit_index)

        final_leader = self.wait_for_stable_leader()
        self.verify_read(
            final_leader,
            after_key,
            "after-failover",
        )
        self.verify_metrics()

        self.log(
            "PASS: leader failover, quorum write/read, restart, "
            "catch-up, and metrics checks succeeded"
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Demonstrate PyRaftKV Docker leader failover "
            "without assuming which node is leader."
        )
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="bounded timeout for each wait phase (default: 60s)",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        help="status polling interval (default: 1s)",
    )
    parser.add_argument(
        "--stable-samples",
        type=int,
        default=3,
        help="consecutive leader observations required (default: 3)",
    )
    parser.add_argument(
        "--no-build",
        action="store_true",
        help="reuse existing node images instead of building",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    compose = DockerCompose()
    demo = FailoverDemo(
        compose,
        ClusterHTTP(),
        timeout=args.timeout,
        poll_interval=args.poll_interval,
        stable_samples=args.stable_samples,
    )

    try:
        demo.run(build=not args.no_build)
    except (DemoError, ValueError) as exc:
        print(f"[demo] ERROR: {exc}", file=sys.stderr)
        demo.diagnostics()

        if demo.stopped_node is not None:
            print(
                f"[demo] restarting {demo.stopped_node} "
                "after failure",
                file=sys.stderr,
            )

            try:
                compose.start(demo.stopped_node)
            except DemoError as restart_error:
                print(
                    f"[demo] restart failed: {restart_error}",
                    file=sys.stderr,
                )

        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
