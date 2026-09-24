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

        self.members = set(members)

        self.followers = self.members - {state.node_id}

        self.next_index = {follower: log.last_index + 1 for follower in self.followers}

        self.match_index = {follower: 0 for follower in self.followers}

    def build_request(
        self,
        follower: str,
        leader_commit: int | None = None,
    ) -> AppendEntriesRequest:
        if follower not in self.followers:
            raise ValueError(f"Unknown follower: {follower}")

        next_index = self.next_index[follower]
        if next_index <= self.log.base_index:
            raise ValueError(
                f"Follower {follower} requires a snapshot"
            )

        prev_log_index = next_index - 1
        prev_log_term = self.log.term_at(prev_log_index)

        if prev_log_term is None:
            raise ValueError(f"Previous log entry {prev_log_index} does not exist")

        entries = tuple(self.log.entries_from(next_index))

        if leader_commit is None:
            leader_commit = self.state.commit_index

        return AppendEntriesRequest(
            term=self.state.current_term,
            leader_id=self.state.node_id,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries,
            leader_commit=leader_commit,
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

        self.match_index[follower] = max(
            self.match_index[follower],
            last_replicated_index,
        )

        self.next_index[follower] = self.match_index[follower] + 1

    def record_snapshot_success(
        self,
        follower: str,
        last_included_index: int,
    ) -> None:
        if follower not in self.followers:
            raise ValueError(f"Unknown follower: {follower}")

        self.match_index[follower] = max(
            self.match_index[follower],
            last_included_index,
        )
        self.next_index[follower] = (
            self.match_index[follower] + 1
        )

    def record_failure(
        self,
        follower: str,
    ) -> None:
        if follower not in self.followers:
            raise ValueError(f"Unknown follower: {follower}")

        self.next_index[follower] = max(
            1,
            self.next_index[follower] - 1,
        )

    def advance_commit_index(self) -> int:
        replicated_indexes = [
            self.log.last_index,
            *self.match_index.values(),
        ]

        replicated_indexes.sort(reverse=True)

        quorum_position = len(self.members) // 2
        candidate_index = replicated_indexes[quorum_position]

        if candidate_index <= self.state.commit_index:
            return self.state.commit_index

        entry = self.log.get(candidate_index)

        if entry is None:
            return self.state.commit_index

        if entry.term != self.state.current_term:
            return self.state.commit_index

        self.state.commit_index = candidate_index

        return self.state.commit_index
