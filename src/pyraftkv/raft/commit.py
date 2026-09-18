from pyraftkv.raft.log import RaftLog
from pyraftkv.raft.state import RaftState
from pyraftkv.raft.state_machine import apply_committed_entries
from pyraftkv.storage.store import KVStore


def update_follower_commit(
    state: RaftState,
    log: RaftLog,
    store: KVStore,
    leader_commit: int,
) -> None:
    if leader_commit <= state.commit_index:
        return

    state.commit_index = min(
        leader_commit,
        log.last_index,
    )

    apply_committed_entries(
        state,
        log,
        store,
    )
