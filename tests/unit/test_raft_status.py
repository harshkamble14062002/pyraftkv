from dataclasses import FrozenInstanceError

import pytest

from pyraftkv.raft.log import RaftCommand
from pyraftkv.raft.node import RaftNode
from pyraftkv.raft.state import NodeRole

MEMBERS = {
    "node-1",
    "node-2",
    "node-3",
}


def test_follower_status_is_immutable_and_detached():
    node = RaftNode("node-2", MEMBERS)

    status = node.status()

    assert status.node_id == "node-2"
    assert status.role == "follower"
    assert status.current_term == 0
    assert status.leader_id is None
    assert status.peers == ("node-1", "node-3")
    assert status.followers is None

    with pytest.raises(FrozenInstanceError):
        status.current_term = 1  # type: ignore[misc]

    payload = status.to_dict()
    payload["peers"].append("unexpected")

    assert node.status().peers == ("node-1", "node-3")


def test_leader_status_reports_follower_progress_and_log_bounds():
    node = RaftNode("node-1", MEMBERS)
    node.state.role = NodeRole.LEADER
    node.state.current_term = 4
    node.state.leader_id = node.node_id
    node.log.append(
        term=4,
        command=RaftCommand(
            operation="PUT",
            key="language",
            value="python",
        ),
    )
    node._initialize_leader_replication()

    assert node.replication is not None

    node.replication.match_index["node-2"] = 1
    node.replication.next_index["node-2"] = 2

    status = node.status()

    assert status.log_base_index == 0
    assert status.log_base_term == 0
    assert status.log_last_index == 1
    assert status.log_last_term == 4
    assert status.snapshot_index == 0
    assert status.snapshot_term == 0
    assert status.followers is not None
    assert [
        follower.to_dict()
        for follower in status.followers
    ] == [
        {
            "node_id": "node-2",
            "match_index": 1,
            "next_index": 2,
            "replication_lag": 0,
        },
        {
            "node_id": "node-3",
            "match_index": 0,
            "next_index": 2,
            "replication_lag": 1,
        },
    ]
