from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class FollowerStatus:
    node_id: str
    match_index: int
    next_index: int
    replication_lag: int

    def to_dict(self) -> dict[str, int | str]:
        return {
            "node_id": self.node_id,
            "match_index": self.match_index,
            "next_index": self.next_index,
            "replication_lag": self.replication_lag,
        }


@dataclass(frozen=True)
class RaftNodeStatus:
    node_id: str
    role: str
    current_term: int
    leader_id: str | None
    commit_index: int
    last_applied: int
    log_base_index: int
    log_base_term: int
    log_last_index: int
    log_last_term: int
    snapshot_index: int
    snapshot_term: int
    peers: tuple[str, ...]
    followers: tuple[FollowerStatus, ...] | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "role": self.role,
            "current_term": self.current_term,
            "leader_id": self.leader_id,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "log_base_index": self.log_base_index,
            "log_base_term": self.log_base_term,
            "log_last_index": self.log_last_index,
            "log_last_term": self.log_last_term,
            "snapshot_index": self.snapshot_index,
            "snapshot_term": self.snapshot_term,
            "peers": list(self.peers),
            "followers": (
                [
                    follower.to_dict()
                    for follower in self.followers
                ]
                if self.followers is not None
                else None
            ),
        }
