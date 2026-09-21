from typing import Protocol

from pyraftkv.raft.rpc import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    RequestVoteRequest,
    RequestVoteResponse,
)


class TransportError(RuntimeError):
    pass


class RaftTransport(Protocol):
    def request_vote(
        self,
        target_id: str,
        request: RequestVoteRequest,
    ) -> RequestVoteResponse: ...

    def append_entries(
        self,
        target_id: str,
        request: AppendEntriesRequest,
    ) -> AppendEntriesResponse: ...
