from pyraftkv.raft.log import RaftLog
from pyraftkv.raft.rpc import AppendEntriesRequest
from pyraftkv.raft.state import NodeRole, RaftState


class LeaderReplication:
    def __init__(
        self,
        state: RaftState,
        log: RaftLog,
        members: set[str],
    ) -> None:
        if state.role != NodeRole.LEADER:
            raise ValueError("Replication state requires a leader")

        if state.node_id not in members:
            raise ValueError("Leader must be a cluster member")

        self.state = state
        self.log = log

        self.followers = members - {state.node_id}

        self.next_index = {follower: log.last_index + 1 for follower in self.followers}

        self.match_index = {follower: 0 for follower in self.followers}

    def build_request(self, follower: str) -> AppendEntriesRequest:
        if follower not in self.followers:
            raise ValueError(f"Unknown follower: {follower}")

        next_index = self.next_index[follower]
        prev_log_index = next_index - 1
        prev_log_term = self.log.term_at(prev_log_index)

        if prev_log_term is None:
            raise ValueError(f"Previous log entry {prev_log_index} does not exist")

        entries = tuple(self.log.entries_from(next_index))

        return AppendEntriesRequest(
            term=self.state.current_term,
            leader_id=self.state.node_id,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries,
        )

    def record_success(
        self,
        follower: str,
        request: AppendEntriesRequest,
    ) -> None:
        if follower not in self.followers:
            raise ValueError(f"Unknown follower: {follower}")

        if request.entries:
            last_replicated_index = request.entries[-1].index
        else:
            last_replicated_index = request.prev_log_index

        self.match_index[follower] = last_replicated_index
        self.next_index[follower] = last_replicated_index + 1

    def record_failure(self, follower: str) -> None:
        if follower not in self.followers:
            raise ValueError(f"Unknown follower: {follower}")

        self.next_index[follower] = max(
            1,
            self.next_index[follower] - 1,
        )
