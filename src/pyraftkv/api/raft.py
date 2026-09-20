from typing import Any

from fastapi import APIRouter, HTTPException

from pyraftkv.raft.node import RaftNode
from pyraftkv.transport.serialization import (
    append_entries_from_dict,
    append_entries_response_to_dict,
    request_vote_from_dict,
    request_vote_response_to_dict,
)


def create_raft_router(
    node: RaftNode,
) -> APIRouter:
    router = APIRouter(
        prefix="/raft",
        tags=["raft"],
    )

    @router.post("/request-vote")
    def request_vote(
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        try:
            request = request_vote_from_dict(
                payload
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise HTTPException(
                status_code=400,
                detail="Invalid RequestVote payload",
            ) from exc

        response = node.handle_request_vote(
            request
        )

        return request_vote_response_to_dict(
            response
        )

    @router.post("/append-entries")
    def append_entries(
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        try:
            request = append_entries_from_dict(
                payload
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise HTTPException(
                status_code=400,
                detail="Invalid AppendEntries payload",
            ) from exc

        response = node.handle_append_entries(
            request
        )

        return append_entries_response_to_dict(
            response
        )

    return router
