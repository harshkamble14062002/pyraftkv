from collections import Counter
from threading import Event, Lock
from typing import Literal

from pyraftkv.raft.rpc import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    InstallSnapshotRequest,
    InstallSnapshotResponse,
    RequestVoteRequest,
    RequestVoteResponse,
    TimeoutNowRequest,
    TimeoutNowResponse,
)
from pyraftkv.transport.base import TransportError
from pyraftkv.transport.node import RaftRPCHandler

RPCName = Literal[
    "request_vote",
    "append_entries",
    "install_snapshot",
    "timeout_now",
]


class InMemoryTransport:
    """In-process Raft transport with deterministic fault injection."""

    def __init__(self) -> None:
        self._nodes: dict[str, RaftRPCHandler] = {}
        self._blocked: set[str] = set()
        self._blocked_links: set[tuple[str, str]] = set()
        self._partition_links: set[tuple[str, str]] = set()
        self._drops: dict[tuple[RPCName, str, str], int] = {}
        self._delays: dict[
            tuple[RPCName, str, str],
            Event,
        ] = {}
        self._calls: Counter[
            tuple[RPCName, str, str]
        ] = Counter()
        self._lock = Lock()

    def register(
        self,
        node_id: str,
        handler: RaftRPCHandler,
    ) -> None:
        with self._lock:
            self._nodes[node_id] = handler

    def unregister(self, node_id: str) -> None:
        with self._lock:
            self._nodes.pop(node_id, None)

    def block(self, node_id: str) -> None:
        """Make a node unreachable in both directions."""
        with self._lock:
            self._blocked.add(node_id)

    def unblock(self, node_id: str) -> None:
        with self._lock:
            self._blocked.discard(node_id)

    def block_link(
        self,
        source_id: str,
        target_id: str,
    ) -> None:
        """Block one directional source-to-target link."""
        with self._lock:
            self._blocked_links.add(
                (source_id, target_id)
            )

    def unblock_link(
        self,
        source_id: str,
        target_id: str,
    ) -> None:
        with self._lock:
            self._blocked_links.discard(
                (source_id, target_id)
            )

    def partition(
        self,
        *groups: set[str],
    ) -> None:
        """Create a symmetric partition between disjoint node groups."""
        if len(groups) < 2:
            raise ValueError(
                "A partition requires at least two groups"
            )

        members: set[str] = set()

        for group in groups:
            if not group:
                raise ValueError(
                    "Partition groups cannot be empty"
                )

            if members.intersection(group):
                raise ValueError(
                    "Partition groups must be disjoint"
                )

            members.update(group)

        with self._lock:
            unknown = members.difference(self._nodes)

            if unknown:
                raise ValueError(
                    f"Unknown partition nodes: {sorted(unknown)}"
                )

            if members != set(self._nodes):
                raise ValueError(
                    "Partition groups must include every node"
                )

            self._partition_links.clear()

            for group_index, group in enumerate(groups):
                for other_group in groups[group_index + 1 :]:
                    for source_id in group:
                        for target_id in other_group:
                            self._partition_links.add(
                                (source_id, target_id)
                            )
                            self._partition_links.add(
                                (target_id, source_id)
                            )

    def heal_partition(self) -> None:
        with self._lock:
            self._partition_links.clear()

    def heal(self) -> None:
        """Remove every injected transport fault and release delays."""
        with self._lock:
            delayed = tuple(self._delays.values())
            self._blocked.clear()
            self._blocked_links.clear()
            self._partition_links.clear()
            self._drops.clear()
            self._delays.clear()

        for release in delayed:
            release.set()

    def drop_next(
        self,
        rpc: RPCName,
        source_id: str,
        target_id: str,
        count: int = 1,
    ) -> None:
        if count <= 0:
            raise ValueError("drop count must be positive")

        with self._lock:
            self._drops[(rpc, source_id, target_id)] = count

    def drop_request_vote(
        self,
        source_id: str,
        target_id: str,
        count: int = 1,
    ) -> None:
        self.drop_next(
            "request_vote",
            source_id,
            target_id,
            count,
        )

    def drop_append_entries(
        self,
        source_id: str,
        target_id: str,
        count: int = 1,
    ) -> None:
        self.drop_next(
            "append_entries",
            source_id,
            target_id,
            count,
        )

    def drop_install_snapshot(
        self,
        source_id: str,
        target_id: str,
        count: int = 1,
    ) -> None:
        self.drop_next(
            "install_snapshot",
            source_id,
            target_id,
            count,
        )

    def drop_timeout_now(
        self,
        source_id: str,
        target_id: str,
        count: int = 1,
    ) -> None:
        self.drop_next(
            "timeout_now",
            source_id,
            target_id,
            count,
        )

    def delay(
        self,
        rpc: RPCName,
        source_id: str,
        target_id: str,
    ) -> Event:
        """Gate matching RPCs until the returned event is set."""
        release = Event()

        with self._lock:
            self._delays[(rpc, source_id, target_id)] = release

        return release

    def delay_request_vote(
        self,
        source_id: str,
        target_id: str,
    ) -> Event:
        return self.delay(
            "request_vote",
            source_id,
            target_id,
        )

    def delay_append_entries(
        self,
        source_id: str,
        target_id: str,
    ) -> Event:
        return self.delay(
            "append_entries",
            source_id,
            target_id,
        )

    def delay_install_snapshot(
        self,
        source_id: str,
        target_id: str,
    ) -> Event:
        return self.delay(
            "install_snapshot",
            source_id,
            target_id,
        )

    def release(
        self,
        rpc: RPCName,
        source_id: str,
        target_id: str,
    ) -> None:
        with self._lock:
            release = self._delays.pop(
                (rpc, source_id, target_id),
                None,
            )

        if release is not None:
            release.set()

    def call_count(
        self,
        rpc: RPCName,
        source_id: str,
        target_id: str,
    ) -> int:
        with self._lock:
            return self._calls[
                (rpc, source_id, target_id)
            ]

    def _get_node(
        self,
        rpc: RPCName,
        source_id: str,
        target_id: str,
    ) -> RaftRPCHandler:
        key = (rpc, source_id, target_id)

        with self._lock:
            self._calls[key] += 1

            if (
                source_id in self._blocked
                or target_id in self._blocked
                or (source_id, target_id)
                in self._blocked_links
                or (source_id, target_id)
                in self._partition_links
            ):
                raise TransportError(
                    f"Link {source_id} -> {target_id} is unreachable"
                )

            drops_remaining = self._drops.get(key, 0)

            if drops_remaining:
                if drops_remaining == 1:
                    del self._drops[key]
                else:
                    self._drops[key] = drops_remaining - 1

                raise TransportError(
                    f"Dropped {rpc} from {source_id} to {target_id}"
                )

            node = self._nodes.get(target_id)

            if node is None:
                raise TransportError(
                    f"Unknown node: {target_id}"
                )

            release = self._delays.get(key)

        if release is not None:
            release.wait()

        return node

    def request_vote(
        self,
        target_id: str,
        request: RequestVoteRequest,
    ) -> RequestVoteResponse:
        node = self._get_node(
            "request_vote",
            request.candidate_id,
            target_id,
        )

        return node.handle_request_vote(request)

    def append_entries(
        self,
        target_id: str,
        request: AppendEntriesRequest,
    ) -> AppendEntriesResponse:
        node = self._get_node(
            "append_entries",
            request.leader_id,
            target_id,
        )

        return node.handle_append_entries(request)

    def timeout_now(
        self,
        target_id: str,
        request: TimeoutNowRequest,
    ) -> TimeoutNowResponse:
        node = self._get_node(
            "timeout_now",
            request.leader_id,
            target_id,
        )

        return node.handle_timeout_now(request)

    def install_snapshot(
        self,
        target_id: str,
        request: InstallSnapshotRequest,
    ) -> InstallSnapshotResponse:
        node = self._get_node(
            "install_snapshot",
            request.leader_id,
            target_id,
        )

        return node.handle_install_snapshot(request)
