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


class InMemoryTransport:
    def __init__(self) -> None:
        self._nodes: dict[str, RaftRPCHandler] = {}
        self._blocked: set[str] = set()

    def register(
        self,
        node_id: str,
        handler: RaftRPCHandler,
    ) -> None:
        self._nodes[node_id] = handler

    def unregister(self, node_id: str) -> None:
        self._nodes.pop(node_id, None)

    def block(self, node_id: str) -> None:
        self._blocked.add(node_id)

    def unblock(self, node_id: str) -> None:
        self._blocked.discard(node_id)

    def _get_node(
        self,
        target_id: str,
    ) -> RaftRPCHandler:
        if target_id in self._blocked:
            raise TransportError(f"Node {target_id} is unreachable")

        node = self._nodes.get(target_id)

        if node is None:
            raise TransportError(f"Unknown node: {target_id}")

        return node

    def request_vote(
        self,
        target_id: str,
        request: RequestVoteRequest,
    ) -> RequestVoteResponse:
        node = self._get_node(target_id)

        return node.handle_request_vote(request)

    def append_entries(
        self,
        target_id: str,
        request: AppendEntriesRequest,
    ) -> AppendEntriesResponse:
        node = self._get_node(target_id)

        return node.handle_append_entries(request)


    def timeout_now(
        self,
        target_id: str,
        request: TimeoutNowRequest,
    ) -> TimeoutNowResponse:
        node = self._get_node(target_id)

        return node.handle_timeout_now(request)


    def install_snapshot(
        self,
        target_id: str,
        request: InstallSnapshotRequest,
    ) -> InstallSnapshotResponse:
        node = self._get_node(target_id)

        return node.handle_install_snapshot(request)
