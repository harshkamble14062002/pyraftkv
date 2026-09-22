from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from pyraftkv.raft.command_processor import (
    CommandQueueFullError,
    RaftCommandProcessor,
)
from pyraftkv.raft.log import RaftCommand
from pyraftkv.raft.node import NotLeaderError, RaftNode
from pyraftkv.transport.base import RaftTransport


class PutRequest(BaseModel):
    value: str


def create_cluster_router(
    node: RaftNode,
    transport: RaftTransport,
    command_processor: RaftCommandProcessor | None = None,
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
            if command_processor is None:
                committed = node.put(
                    key,
                    body.value,
                    transport,
                )
            else:
                committed = command_processor.submit(
                    RaftCommand(
                        operation="PUT",
                        key=key,
                        value=body.value,
                    )
                )
        except CommandQueueFullError as exc:
            raise HTTPException(
                status_code=429,
                detail={
                    "error": "command_queue_full",
                },
            ) from exc
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
            if command_processor is None:
                committed = node.delete(
                    key,
                    transport,
                )
            else:
                committed = command_processor.submit(
                    RaftCommand(
                        operation="DELETE",
                        key=key,
                    )
                )
        except CommandQueueFullError as exc:
            raise HTTPException(
                status_code=429,
                detail={
                    "error": "command_queue_full",
                },
            ) from exc
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
