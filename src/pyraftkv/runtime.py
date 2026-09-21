import argparse
import asyncio
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass

import uvicorn
from fastapi import FastAPI

from pyraftkv.api.cluster import create_cluster_router
from pyraftkv.api.raft import create_raft_router
from pyraftkv.raft.node import RaftNode
from pyraftkv.raft.state import NodeRole
from pyraftkv.transport.http import HTTPTransport

logger = logging.getLogger(__name__)


@dataclass
class NodeRuntime:
    node: RaftNode
    transport: HTTPTransport


def parse_peer(value: str) -> tuple[str, str]:
    try:
        node_id, address = value.split("=", 1)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Peer must use NODE_ID=URL format") from exc

    node_id = node_id.strip()
    address = address.strip().rstrip("/")

    if not node_id:
        raise argparse.ArgumentTypeError("Peer node ID cannot be empty")

    if not address.startswith(("http://", "https://")):
        raise argparse.ArgumentTypeError(
            "Peer address must start with http:// or https://"
        )

    return node_id, address


def create_runtime(
    node_id: str,
    peers: dict[str, str],
) -> NodeRuntime:
    if node_id in peers:
        raise ValueError("Local node must not be listed as its own peer")

    members = {
        node_id,
        *peers.keys(),
    }

    node = RaftNode(
        node_id=node_id,
        members=members,
    )

    transport = HTTPTransport(peers)

    return NodeRuntime(
        node=node,
        transport=transport,
    )


def run_raft_iteration(
    runtime: NodeRuntime,
) -> None:
    node = runtime.node

    previous_role = node.state.role
    previous_term = node.state.current_term

    if node.state.role == NodeRole.LEADER:
        node.send_heartbeats(runtime.transport)
    else:
        node.tick(runtime.transport)

    if node.state.role != previous_role or node.state.current_term != previous_term:
        logger.info(
            "Raft state changed: node=%s role=%s term=%s leader=%s",
            node.node_id,
            node.state.role.value,
            node.state.current_term,
            node.state.leader_id,
        )


async def raft_background_loop(
    runtime: NodeRuntime,
    interval: float = 0.05,
) -> None:
    while True:
        try:
            await asyncio.to_thread(
                run_raft_iteration,
                runtime,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Raft background iteration failed")

        await asyncio.sleep(interval)


def create_node_app(
    runtime: NodeRuntime,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(
        app: FastAPI,
    ):
        app.state.runtime = runtime

        raft_task = asyncio.create_task(raft_background_loop(runtime))

        app.state.raft_task = raft_task

        try:
            yield
        finally:
            raft_task.cancel()

            try:
                await raft_task
            except asyncio.CancelledError:
                pass

            runtime.transport.close()

    app = FastAPI(
        title="PyRaftKV",
        lifespan=lifespan,
    )

    app.include_router(create_raft_router(runtime.node))

    app.include_router(
        create_cluster_router(
            runtime.node,
            runtime.transport,
        )
    )

    @app.get("/health")
    def health() -> dict[str, object]:
        state = runtime.node.state

        return {
            "status": "ok",
            "node_id": runtime.node.node_id,
            "role": state.role.value,
            "term": state.current_term,
            "leader_id": state.leader_id,
            "commit_index": state.commit_index,
            "last_applied": state.last_applied,
        }

    return app


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a PyRaftKV node")

    parser.add_argument(
        "--node-id",
        required=True,
    )

    parser.add_argument(
        "--host",
        default="127.0.0.1",
    )

    parser.add_argument(
        "--port",
        required=True,
        type=int,
    )

    parser.add_argument(
        "--peer",
        action="append",
        default=[],
        type=parse_peer,
        metavar="NODE_ID=URL",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    peers: dict[str, str] = {}

    for peer_id, address in args.peer:
        if peer_id in peers:
            parser.error(f"Duplicate peer: {peer_id}")

        peers[peer_id] = address

    if args.node_id in peers:
        parser.error("Local node cannot also be configured as a peer")

    runtime = create_runtime(
        node_id=args.node_id,
        peers=peers,
    )

    app = create_node_app(runtime)

    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
    )


if __name__ == "__main__":
    main()
