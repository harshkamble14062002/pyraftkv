from dataclasses import dataclass
from enum import Enum


class NodeRole(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class RaftState:
    node_id: str
    commit_index: int = 0
    
    current_term: int = 0
    voted_for: str | None = None
    leader_id: str | None = None
    role: NodeRole = NodeRole.FOLLOWER

    last_applied: int = 0

    def become_follower(
        self,
        term: int,
        leader_id: str | None = None,
    ) -> None:
        if term < self.current_term:
            raise ValueError("Cannot move to an older term")

        if term > self.current_term:
            self.voted_for = None

        self.current_term = term
        self.role = NodeRole.FOLLOWER
        self.leader_id = leader_id

    def become_candidate(self) -> None:
        self.current_term += 1
        self.role = NodeRole.CANDIDATE
        self.voted_for = self.node_id
        self.leader_id = None

    def become_leader(self) -> None:
        if self.role != NodeRole.CANDIDATE:
            raise RuntimeError("Only a candidate can become leader")

        self.role = NodeRole.LEADER
        self.leader_id = self.node_id
