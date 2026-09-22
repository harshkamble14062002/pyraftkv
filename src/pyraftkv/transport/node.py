from typing import Protocol

from pyraftkv.raft.rpc import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    RequestVoteRequest,
    RequestVoteResponse,
    TimeoutNowRequest,
    TimeoutNowResponse,
)


class RaftRPCHandler(Protocol):
    def handle_request_vote(
        self,
        request: RequestVoteRequest,
    ) -> RequestVoteResponse: ...

    def handle_append_entries(
        self,
        request: AppendEntriesRequest,
    ) -> AppendEntriesResponse: ...


    def handle_timeout_now(
        self,
        request: TimeoutNowRequest,
    ) -> TimeoutNowResponse: ...
