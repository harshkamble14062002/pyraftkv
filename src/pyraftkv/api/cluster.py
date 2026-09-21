from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from pyraftkv.raft.node import NotLeaderError, RaftNode
from pyraftkv.transport.base import RaftTransport


class PutRequest(BaseModel):
    value: str


def create_cluster_router(
    node: RaftNode,
    transport: RaftTransport,
) -> APIRouter:
    router = APIRouter(
        prefix="/kv",
        tags=["kv"],
    )

    @router.put("/{key}")
    def put_key(
        key: str,
        body: PutRequest,
    ) -> dict[str, Any]:
        try:
            committed = node.put(
                key,
                body.value,
                transport,
            )
        except NotLeaderError as exc:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "not_leader",
                    "leader_id": node.state.leader_id,
                },
            ) from exc

        if not committed:
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "quorum_unavailable",
                },
            )

        return {
            "key": key,
            "value": body.value,
            "committed": True,
            "leader_id": node.node_id,
        }

    @router.get("/{key}")
    def get_key(
        key: str,
    ) -> dict[str, Any]:
        value = node.store.get(key)

        if value is None:
            raise HTTPException(
                status_code=404,
                detail="Key not found",
            )

        return {
            "key": key,
            "value": value,
            "node_id": node.node_id,
            "leader_id": node.state.leader_id,
        }

    @router.delete("/{key}")
    def delete_key(
        key: str,
    ) -> dict[str, Any]:
        try:
            committed = node.delete(
                key,
                transport,
            )
        except NotLeaderError as exc:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "not_leader",
                    "leader_id": node.state.leader_id,
                },
            ) from exc

        if not committed:
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "quorum_unavailable",
                },
            )

        return {
            "key": key,
            "deleted": True,
            "committed": True,
            "leader_id": node.node_id,
        }

    return router
