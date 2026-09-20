from fastapi import FastAPI

from pyraftkv.api.raft import create_raft_router
from pyraftkv.raft.node import RaftNode


def create_raft_app(
    node: RaftNode,
) -> FastAPI:
    app = FastAPI(
        title="PyRaftKV Raft API"
    )

    app.include_router(
        create_raft_router(node)
    )

    return app
