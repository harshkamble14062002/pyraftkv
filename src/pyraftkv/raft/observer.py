from typing import Protocol


class RaftObserver(Protocol):
    """Receives bounded-cardinality Raft operational events."""

    def record_election_attempt(self) -> None:
        """Record the start of a local election."""

    def record_leadership_change(self) -> None:
        """Record this node becoming leader."""

    def record_rpc(
        self,
        rpc: str,
        duration_seconds: float,
        success: bool,
    ) -> None:
        """Record an outbound Raft RPC."""

    def record_read_barrier(
        self,
        duration_seconds: float,
        success: bool,
    ) -> None:
        """Record a linearizable-read quorum barrier."""

    def record_snapshot_installation(
        self,
        duration_seconds: float,
        size_bytes: int,
    ) -> None:
        """Record a successfully installed snapshot."""

    def record_log_compaction(self) -> None:
        """Record a successful local log compaction."""
