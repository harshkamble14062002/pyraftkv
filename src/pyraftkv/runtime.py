import argparse
import asyncio
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST

from pyraftkv.api.cluster import create_cluster_router
from pyraftkv.api.raft import create_raft_router
from pyraftkv.observability.metrics import RaftMetrics
from pyraftkv.raft.command_processor import RaftCommandProcessor
from pyraftkv.raft.node import RaftNode
from pyraftkv.raft.state import NodeRole
from pyraftkv.transport.http import HTTPTransport

logger = logging.getLogger(__name__)


@dataclass
class NodeRuntime:
    node: RaftNode
    transport: HTTPTransport
    metrics: RaftMetrics | None = None
    command_processor: RaftCommandProcessor | None = None


def parse_peer(value: str) -> tuple[str, str]:
    try:
        node_id, address = value.split("=", 1)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "Peer must use NODE_ID=URL format"
        ) from exc

    node_id = node_id.strip()
    address = address.strip().rstrip("/")

    if not node_id:
        raise argparse.ArgumentTypeError(
            "Peer node ID cannot be empty"
        )

    if not address.startswith(("http://", "https://")):
        raise argparse.ArgumentTypeError(
            "Peer address must start with http:// or https://"
        )

    return node_id, address


def create_runtime(
    node_id: str,
    peers: dict[str, str],
    data_dir: str | Path | None = None,
) -> NodeRuntime:
    if node_id in peers:
        raise ValueError(
            "Local node must not be listed as its own peer"
        )

    members = {
        node_id,
        *peers.keys(),
    }

    if data_dir is None:
        data_dir = Path("data") / node_id

    node = RaftNode(
        node_id=node_id,
        members=members,
        data_dir=data_dir,
    )

    transport = HTTPTransport(peers)

    metrics = RaftMetrics(
        node_id=node_id,
    )

    metrics.update_from_node(node)

    return NodeRuntime(
        node=node,
        transport=transport,
        metrics=metrics,
        command_processor=RaftCommandProcessor(
            node,
            transport,
        ),
    )


def run_raft_iteration(
    runtime: NodeRuntime,
) -> None:
    node = runtime.node

    previous_role = node.state.role
    previous_term = node.state.current_term

    if node.state.role == NodeRole.LEADER:
        node.replicate_log(runtime.transport)
    else:
        node.tick(runtime.transport)

    if (
        node.state.role != previous_role
        or node.state.current_term != previous_term
    ):
        logger.info(
            "Raft state changed: node=%s role=%s term=%s leader=%s",
            node.node_id,
            node.state.role.value,
            node.state.current_term,
            node.state.leader_id,
        )

    if runtime.metrics is not None:
        runtime.metrics.update_from_node(node)


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
            logger.exception(
                "Raft background iteration failed"
            )

        await asyncio.sleep(interval)


def create_node_app(
    runtime: NodeRuntime,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(
        app: FastAPI,
    ):
        app.state.runtime = runtime

        if runtime.command_processor is not None:
            runtime.command_processor.start()

        raft_task = asyncio.create_task(
            raft_background_loop(runtime)
        )

        app.state.raft_task = raft_task

        try:
            yield
        finally:
            raft_task.cancel()

            try:
                await raft_task
            except asyncio.CancelledError:
                pass

            if runtime.command_processor is not None:
                runtime.command_processor.stop()

            runtime.transport.close()

    app = FastAPI(
        title="PyRaftKV",
        lifespan=lifespan,
    )

    if runtime.metrics is None:
        runtime.metrics = RaftMetrics(
            node_id=runtime.node.node_id,
        )

    runtime.metrics.update_from_node(
        runtime.node
    )

    @app.middleware("http")
    async def record_http_metrics(
        request: Request,
        call_next,
    ):
        start = time.perf_counter()

        response = await call_next(request)

        duration = time.perf_counter() - start

        if runtime.metrics is not None:
            path = request.url.path

            runtime.metrics.http_requests.labels(
                node_id=runtime.node.node_id,
                method=request.method,
                path=path,
                status=str(response.status_code),
            ).inc()

            runtime.metrics.http_latency.labels(
                node_id=runtime.node.node_id,
                method=request.method,
                path=path,
            ).observe(duration)

        return response

    @app.get("/metrics")
    def metrics() -> Response:
        assert runtime.metrics is not None

        runtime.metrics.update_from_node(
            runtime.node
        )

        return Response(
            content=runtime.metrics.render(),
            media_type=CONTENT_TYPE_LATEST,
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

    app.include_router(
        create_raft_router(runtime.node)
    )

    app.include_router(
        create_cluster_router(
            runtime.node,
            runtime.transport,
            runtime.command_processor,
        )
    )

    return app


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a PyRaftKV node"
    )

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

    parser.add_argument(
        "--data-dir",
        default=None,
        help="Directory for persistent Raft state",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    peers: dict[str, str] = {}

    for peer_id, address in args.peer:
        if peer_id in peers:
            parser.error(
                f"Duplicate peer: {peer_id}"
            )

        peers[peer_id] = address

    if args.node_id in peers:
        parser.error(
            "Local node cannot also be configured as a peer"
        )

    runtime = create_runtime(
        node_id=args.node_id,
        peers=peers,
        data_dir=args.data_dir,
    )

    app = create_node_app(runtime)

    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
    )


if __name__ == "__main__":
    main()