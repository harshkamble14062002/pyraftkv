from concurrent.futures import ThreadPoolExecutor

from pyraftkv.raft.log import (
    LogEntry,
    RaftCommand,
    RaftLog,
)
from pyraftkv.raft.persistence import (
    RaftPersistence,
)
from pyraftkv.raft.state import RaftState


def test_raft_state_round_trip(tmp_path):
    persistence = RaftPersistence(tmp_path)

    state = RaftState(
        node_id="node-1",
        current_term=7,
        voted_for="node-2",
        commit_index=3,
    )

    persistence.save_state(state)

    restored = persistence.load_state("node-1")

    assert restored.current_term == 7
    assert restored.voted_for == "node-2"
    assert restored.commit_index == 3


def test_raft_log_round_trip(tmp_path):
    persistence = RaftPersistence(tmp_path)

    log = RaftLog()

    log.append(
        term=1,
        command=RaftCommand(
            operation="PUT",
            key="name",
            value="harsha",
        ),
    )

    log.append(
        term=2,
        command=RaftCommand(
            operation="DELETE",
            key="name",
        ),
    )

    persistence.save_log(log)

    restored = persistence.load_log()

    assert restored.last_index == 2
    assert restored.get(1) == log.get(1)
    assert restored.get(2) == log.get(2)


def test_concurrent_log_saves_are_safe(
    tmp_path,
):
    persistence = RaftPersistence(
        tmp_path
    )

    log = RaftLog()

    log.append(
        term=1,
        command=RaftCommand(
            operation="PUT",
            key="x",
            value="10",
        ),
    )

    def save(_: int) -> None:
        persistence.save_log(log)

    with ThreadPoolExecutor(
        max_workers=10
    ) as executor:
        list(
            executor.map(
                save,
                range(100),
            )
        )

    restored = persistence.load_log()

    assert restored.last_index == 1
    assert restored.get(1) == log.get(1)

def test_append_log_entries_survive_restart(
    tmp_path,
):
    persistence = RaftPersistence(
        tmp_path
    )

    entry_1 = LogEntry(
        index=1,
        term=1,
        command=RaftCommand(
            operation="PUT",
            key="name",
            value="harsha",
        ),
    )

    entry_2 = LogEntry(
        index=2,
        term=1,
        command=RaftCommand(
            operation="PUT",
            key="language",
            value="python",
        ),
    )

    persistence.append_log_entries(
        [entry_1]
    )

    persistence.append_log_entries(
        [entry_2]
    )

    restarted = RaftPersistence(
        tmp_path
    )

    restored = restarted.load_log()

    assert restored.last_index == 2
    assert restored.get(1) == entry_1
    assert restored.get(2) == entry_2


def test_truncate_log_survives_restart(
    tmp_path,
):
    persistence = RaftPersistence(
        tmp_path
    )

    entry_1 = LogEntry(
        index=1,
        term=1,
        command=RaftCommand(
            operation="PUT",
            key="a",
            value="1",
        ),
    )

    old_entry_2 = LogEntry(
        index=2,
        term=1,
        command=RaftCommand(
            operation="PUT",
            key="b",
            value="old",
        ),
    )

    replacement_entry_2 = LogEntry(
        index=2,
        term=2,
        command=RaftCommand(
            operation="PUT",
            key="b",
            value="new",
        ),
    )

    persistence.append_log_entries(
        [
            entry_1,
            old_entry_2,
        ]
    )

    persistence.truncate_log_from(
        2
    )

    persistence.append_log_entries(
        [
            replacement_entry_2,
        ]
    )

    restarted = RaftPersistence(
        tmp_path
    )

    restored = restarted.load_log()

    assert restored.last_index == 2
    assert restored.get(1) == entry_1
    assert restored.get(2) == replacement_entry_2