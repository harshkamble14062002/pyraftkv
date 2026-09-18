from pyraftkv.raft.log import RaftCommand, RaftLog
from pyraftkv.raft.state import RaftState
from pyraftkv.storage.store import KVStore


def apply_command(
    store: KVStore,
    command: RaftCommand,
) -> None:
    if command.operation == "PUT":
        if command.value is None:
            raise ValueError("PUT command requires a value")

        store.put(
            command.key,
            command.value,
        )

    elif command.operation == "DELETE":
        store.delete(command.key)

    else:
        raise ValueError(
            f"Unknown Raft command: {command.operation}"
        )


def apply_committed_entries(
    state: RaftState,
    log: RaftLog,
    store: KVStore,
) -> None:
    while state.last_applied < state.commit_index:
        next_index = state.last_applied + 1

        entry = log.get(next_index)

        if entry is None:
            raise RuntimeError(
                f"Missing committed log entry {next_index}"
            )

        apply_command(
            store,
            entry.command,
        )

        state.last_applied = next_index

