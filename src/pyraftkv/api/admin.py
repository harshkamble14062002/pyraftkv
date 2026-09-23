from typing import Any

from fastapi import APIRouter

from pyraftkv.raft.node import RaftNode


def create_admin_router(
    node: RaftNode,
) -> APIRouter:
    """Create read-only cluster diagnostics routes."""
    router = APIRouter(
        prefix="/cluster",
        tags=["cluster"],
    )

    @router.get("/status")
    def cluster_status() -> dict[str, Any]:
        return node.status().to_dict()

    return router
