from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

from pyraftkv.raft.node import RaftNode
from pyraftkv.raft.state import NodeRole

_VALID_RPC_NAMES = frozenset(
    {
        "append_entries",
        "install_snapshot",
        "request_vote",
        "timeout_now",
    }
)


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

        self.snapshot_index = Gauge(
            "pyraftkv_raft_snapshot_index",
            "Last index included in the local snapshot",
            ["node_id"],
            registry=self.registry,
        )

        self.election_attempts = Counter(
            "pyraftkv_raft_election_attempts_total",
            "Total local Raft election attempts",
            ["node_id"],
            registry=self.registry,
        )

        self.leadership_changes = Counter(
            "pyraftkv_raft_leadership_changes_total",
            "Total times this node became Raft leader",
            ["node_id"],
            registry=self.registry,
        )

        self.rpc_failures = Counter(
            "pyraftkv_raft_rpc_failures_total",
            "Total outbound Raft transport failures",
            ["node_id", "rpc"],
            registry=self.registry,
        )

        self.rpc_latency = Histogram(
            "pyraftkv_raft_rpc_duration_seconds",
            "Outbound Raft RPC latency",
            ["node_id", "rpc"],
            registry=self.registry,
        )

        self.follower_replication_lag = Gauge(
            "pyraftkv_raft_follower_replication_lag",
            "Leader log entries not matched by each follower",
            ["node_id", "follower_id"],
            registry=self.registry,
        )

        self.read_barrier_attempts = Counter(
            "pyraftkv_raft_read_barrier_attempts_total",
            "Total linearizable read quorum barriers",
            ["node_id"],
            registry=self.registry,
        )

        self.read_barrier_failures = Counter(
            "pyraftkv_raft_read_barrier_failures_total",
            "Failed linearizable read quorum barriers",
            ["node_id"],
            registry=self.registry,
        )

        self.read_barrier_latency = Histogram(
            "pyraftkv_raft_read_barrier_duration_seconds",
            "Linearizable read quorum barrier latency",
            ["node_id"],
            registry=self.registry,
        )

        self.snapshot_installations = Counter(
            "pyraftkv_raft_snapshot_installations_total",
            "Total snapshots installed successfully",
            ["node_id"],
            registry=self.registry,
        )

        self.snapshot_install_bytes = Counter(
            "pyraftkv_raft_snapshot_install_bytes_total",
            "Serialized state bytes installed from snapshots",
            ["node_id"],
            registry=self.registry,
        )

        self.snapshot_install_latency = Histogram(
            "pyraftkv_raft_snapshot_install_duration_seconds",
            "Successful snapshot installation latency",
            ["node_id"],
            registry=self.registry,
        )

        self.log_compactions = Counter(
            "pyraftkv_raft_log_compactions_total",
            "Total successful local Raft log compactions",
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

        self._follower_labels: set[str] = set()

    def record_election_attempt(self) -> None:
        self.election_attempts.labels(
            node_id=self.node_id
        ).inc()

    def record_leadership_change(self) -> None:
        self.leadership_changes.labels(
            node_id=self.node_id
        ).inc()

    def record_rpc(
        self,
        rpc: str,
        duration_seconds: float,
        success: bool,
    ) -> None:
        if rpc not in _VALID_RPC_NAMES:
            raise ValueError(
                f"Unsupported Raft RPC metric label: {rpc}"
            )

        self.rpc_latency.labels(
            node_id=self.node_id,
            rpc=rpc,
        ).observe(duration_seconds)

        if not success:
            self.rpc_failures.labels(
                node_id=self.node_id,
                rpc=rpc,
            ).inc()

    def record_read_barrier(
        self,
        duration_seconds: float,
        success: bool,
    ) -> None:
        self.read_barrier_attempts.labels(
            node_id=self.node_id
        ).inc()
        self.read_barrier_latency.labels(
            node_id=self.node_id
        ).observe(duration_seconds)

        if not success:
            self.read_barrier_failures.labels(
                node_id=self.node_id
            ).inc()

    def record_snapshot_installation(
        self,
        duration_seconds: float,
        size_bytes: int,
    ) -> None:
        self.snapshot_installations.labels(
            node_id=self.node_id
        ).inc()
        self.snapshot_install_bytes.labels(
            node_id=self.node_id
        ).inc(size_bytes)
        self.snapshot_install_latency.labels(
            node_id=self.node_id
        ).observe(duration_seconds)

    def record_log_compaction(self) -> None:
        self.log_compactions.labels(
            node_id=self.node_id
        ).inc()

    def update_from_node(
        self,
        node: RaftNode,
    ) -> None:
        status = node.status()

        self.current_term.labels(
            node_id=self.node_id
        ).set(
            status.current_term
        )

        self.commit_index.labels(
            node_id=self.node_id
        ).set(
            status.commit_index
        )

        self.last_applied.labels(
            node_id=self.node_id
        ).set(
            status.last_applied
        )

        self.log_last_index.labels(
            node_id=self.node_id
        ).set(
            status.log_last_index
        )

        self.snapshot_index.labels(
            node_id=self.node_id
        ).set(
            status.snapshot_index
        )

        for role in NodeRole:
            self.role.labels(
                node_id=self.node_id,
                role=role.value,
            ).set(
                1
                if status.role == role.value
                else 0
            )

        follower_labels: set[str] = set()

        for follower in status.followers or ():
            follower_labels.add(follower.node_id)
            self.follower_replication_lag.labels(
                node_id=self.node_id,
                follower_id=follower.node_id,
            ).set(follower.replication_lag)

        for follower_id in (
            self._follower_labels - follower_labels
        ):
            self.follower_replication_lag.remove(
                self.node_id,
                follower_id,
            )

        self._follower_labels = follower_labels

    def render(self) -> bytes:
        return generate_latest(
            self.registry
        )