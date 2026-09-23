from collections.abc import Callable
from pathlib import Path
from typing import Self

from pyraftkv.raft.node import NotLeaderError, RaftNode
from pyraftkv.raft.state import NodeRole
from pyraftkv.transport.memory import InMemoryTransport


class RaftCluster:
    """Small deterministic cluster harness for invariant-oriented tests."""

    def __init__(
        self,
        size: int = 3,
        data_dir: Path | None = None,
    ) -> None:
        if size < 1:
            raise ValueError("Cluster size must be positive")

        self.node_ids = tuple(
            f"node-{index}"
            for index in range(1, size + 1)
        )
        self.members = set(self.node_ids)
        self.data_dir = data_dir
        self.transport = InMemoryTransport()
        self.nodes = {
            node_id: self._new_node(node_id)
            for node_id in self.node_ids
        }
        self._last_commit = {
            node_id: node.state.commit_index
            for node_id, node in self.nodes.items()
        }
        self._leaders_by_term: dict[int, set[str]] = {}

        for node_id, node in self.nodes.items():
            self.transport.register(node_id, node)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ) -> None:
        self.close()

    def _new_node(self, node_id: str) -> RaftNode:
        node_dir = (
            self.data_dir / node_id
            if self.data_dir is not None
            else None
        )

        return RaftNode(
            node_id,
            self.members,
            data_dir=node_dir,
        )

    def elect_leader(self, node_id: str) -> RaftNode:
        node = self.nodes[node_id]
        node.timer.expire_now()
        node.tick(self.transport)

        if node.state.role != NodeRole.LEADER:
            raise AssertionError(
                f"{node_id} was not elected; {self.diagnostics()}"
            )

        self.assert_invariants()

        return node

    def current_leaders(self) -> list[RaftNode]:
        return [
            self.nodes[node_id]
            for node_id in self.node_ids
            if self.nodes[node_id].state.role
            == NodeRole.LEADER
        ]

    def partition(
        self,
        *groups: set[str],
    ) -> None:
        self.transport.partition(*groups)

    def isolate(self, node_id: str) -> None:
        self.partition(
            {node_id},
            self.members - {node_id},
        )

    def heal(self) -> None:
        self.transport.heal()

    def run_rounds(self, count: int = 1) -> None:
        if count < 0:
            raise ValueError("Round count cannot be negative")

        for _ in range(count):
            for leader in self.current_leaders():
                try:
                    leader.replicate_log(self.transport)
                except NotLeaderError:
                    continue

            self.assert_invariants()

    def run_until(
        self,
        predicate: Callable[[], bool],
        max_rounds: int = 50,
    ) -> None:
        for _ in range(max_rounds + 1):
            if predicate():
                return

            self.run_rounds()

        raise AssertionError(
            "Cluster did not converge within "
            f"{max_rounds} rounds; {self.diagnostics()}"
        )

    def restart_node(self, node_id: str) -> RaftNode:
        if self.data_dir is None:
            raise RuntimeError(
                "Persistent data_dir is required for restart tests"
            )

        old_node = self.nodes[node_id]
        old_node.close()
        self.transport.unregister(node_id)

        restarted = self._new_node(node_id)
        self.nodes[node_id] = restarted
        self._last_commit[node_id] = (
            restarted.state.commit_index
        )
        self.transport.register(node_id, restarted)

        return restarted

    def assert_invariants(self) -> None:
        for node_id, node in self.nodes.items():
            if (
                node.state.last_applied
                > node.state.commit_index
            ):
                raise AssertionError(
                    f"{node_id} applied beyond commit; "
                    f"{self.diagnostics()}"
                )

            if node.state.commit_index > node.log.last_index:
                raise AssertionError(
                    f"{node_id} committed beyond its log; "
                    f"{self.diagnostics()}"
                )

            previous_commit = self._last_commit[node_id]

            if node.state.commit_index < previous_commit:
                raise AssertionError(
                    f"{node_id} commit index decreased; "
                    f"{self.diagnostics()}"
                )

            self._last_commit[node_id] = (
                node.state.commit_index
            )

            if node.state.role == NodeRole.LEADER:
                leaders = self._leaders_by_term.setdefault(
                    node.state.current_term,
                    set(),
                )
                leaders.add(node_id)

                if len(leaders) > 1:
                    raise AssertionError(
                        "Multiple observed leaders in term "
                        f"{node.state.current_term}: "
                        f"{sorted(leaders)}"
                    )

    def assert_converged(
        self,
        node_ids: set[str] | None = None,
    ) -> None:
        selected = tuple(
            sorted(node_ids or self.members)
        )
        expected = self.nodes[selected[0]]
        expected_state = expected.store.snapshot()

        for node_id in selected[1:]:
            node = self.nodes[node_id]

            if node.store.snapshot() != expected_state:
                raise AssertionError(
                    f"KV state differs on {node_id}; "
                    f"{self.diagnostics()}"
                )

            if (
                node.state.commit_index
                != expected.state.commit_index
                or node.state.last_applied
                != expected.state.last_applied
            ):
                raise AssertionError(
                    f"Commit/apply state differs on {node_id}; "
                    f"{self.diagnostics()}"
                )

        self.assert_invariants()

    def diagnostics(self) -> str:
        return "; ".join(
            (
                f"{node_id}:role={node.state.role.value},"
                f"term={node.state.current_term},"
                f"leader={node.state.leader_id},"
                f"commit={node.state.commit_index},"
                f"applied={node.state.last_applied},"
                f"last={node.log.last_index}"
            )
            for node_id, node in sorted(self.nodes.items())
        )

    def close(self) -> None:
        self.transport.heal()

        for node in self.nodes.values():
            node.close()
