from typing import Protocol

from pyraftkv.raft.rpc import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    RequestVoteRequest,
    RequestVoteResponse,
    TimeoutNowRequest,
    TimeoutNowResponse,
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


    def timeout_now(
        self,
        target_id: str,
        request: TimeoutNowRequest,
    ) -> TimeoutNowResponse: ...
