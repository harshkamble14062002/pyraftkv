from unittest.mock import Mock

from pyraftkv.raft.state import NodeRole
from pyraftkv.runtime import (
    NodeRuntime,
    run_raft_iteration,
)


def test_follower_iteration_checks_election():
    node = Mock()
    transport = Mock()

    node.state.role = NodeRole.FOLLOWER
    node.state.current_term = 0

    runtime = NodeRuntime(
        node=node,
        transport=transport,
    )

    run_raft_iteration(runtime)

    node.tick.assert_called_once_with(transport)

    node.send_heartbeats.assert_not_called()


def test_candidate_iteration_checks_election():
    node = Mock()
    transport = Mock()

    node.state.role = NodeRole.CANDIDATE
    node.state.current_term = 2

    runtime = NodeRuntime(
        node=node,
        transport=transport,
    )

    run_raft_iteration(runtime)

    node.tick.assert_called_once_with(transport)


def test_leader_iteration_sends_heartbeats():
    node = Mock()
    transport = Mock()

    node.state.role = NodeRole.LEADER
    node.state.current_term = 3

    runtime = NodeRuntime(
        node=node,
        transport=transport,
    )

    run_raft_iteration(runtime)

    node.send_heartbeats.assert_called_once_with(transport)

    node.tick.assert_not_called()


import asyncio

import pytest

from pyraftkv.runtime import raft_background_loop


@pytest.mark.anyio
async def test_background_loop_can_be_cancelled():
    node = Mock()
    transport = Mock()

    node.state.role = NodeRole.FOLLOWER
    node.state.current_term = 0
    node.state.leader_id = None
    node.node_id = "node-1"

    runtime = NodeRuntime(
        node=node,
        transport=transport,
    )

    task = asyncio.create_task(
        raft_background_loop(
            runtime,
            interval=0.001,
        )
    )

    await asyncio.sleep(0.01)

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
