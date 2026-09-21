from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

from pyraftkv.raft.node import RaftNode
from pyraftkv.raft.state import NodeRole


class RaftMetrics:
    def __init__(
        self,
        node_id: str,
    ) -> None:
        self.node_id = node_id

        self.registry = CollectorRegistry()

        self.current_term = Gauge(
            "pyraftkv_raft_current_term",
            "Current Raft term",
            ["node_id"],
            registry=self.registry,
        )

        self.role = Gauge(
            "pyraftkv_raft_role",
            "Current Raft role",
            ["node_id", "role"],
            registry=self.registry,
        )

        self.commit_index = Gauge(
            "pyraftkv_raft_commit_index",
            "Highest committed Raft log index",
            ["node_id"],
            registry=self.registry,
        )

        self.last_applied = Gauge(
            "pyraftkv_raft_last_applied",
            "Highest log index applied to the state machine",
            ["node_id"],
            registry=self.registry,
        )

        self.log_last_index = Gauge(
            "pyraftkv_raft_log_last_index",
            "Last local Raft log index",
            ["node_id"],
            registry=self.registry,
        )

        self.http_requests = Counter(
            "pyraftkv_http_requests_total",
            "Total HTTP requests",
            [
                "node_id",
                "method",
                "path",
                "status",
            ],
            registry=self.registry,
        )

        self.http_latency = Histogram(
            "pyraftkv_http_request_duration_seconds",
            "HTTP request latency",
            [
                "node_id",
                "method",
                "path",
            ],
            registry=self.registry,
        )

    def update_from_node(
        self,
        node: RaftNode,
    ) -> None:
        self.current_term.labels(
            node_id=self.node_id
        ).set(
            node.state.current_term
        )

        self.commit_index.labels(
            node_id=self.node_id
        ).set(
            node.state.commit_index
        )

        self.last_applied.labels(
            node_id=self.node_id
        ).set(
            node.state.last_applied
        )

        self.log_last_index.labels(
            node_id=self.node_id
        ).set(
            node.log.last_index
        )

        for role in NodeRole:
            self.role.labels(
                node_id=self.node_id,
                role=role.value,
            ).set(
                1
                if node.state.role == role
                else 0
            )

    def render(self) -> bytes:
        return generate_latest(
            self.registry
        )