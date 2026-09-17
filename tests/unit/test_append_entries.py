from pyraftkv.raft.log import LogEntry, RaftCommand, RaftLog
from pyraftkv.raft.replication import handle_append_entries
from pyraftkv.raft.rpc import AppendEntriesRequest
from pyraftkv.raft.state import RaftState


def entry(
    index: int,
    term: int,
    key: str,
) -> LogEntry:
    return LogEntry(
        index=index,
        term=term,
        command=RaftCommand(
            operation="PUT",
            key=key,
            value=key,
        ),
    )

def test_rejects_old_term():
    state = RaftState(
        node_id="node-2",
        current_term=5,
    )
    log = RaftLog()

    response = handle_append_entries(
        state,
        log,
        AppendEntriesRequest(
            term=4,
            leader_id="node-1",
        ),
    )

    assert response.success is False
    assert response.term == 5


def test_rejects_missing_previous_entry():
    state = RaftState(node_id="node-2")
    log = RaftLog()

    response = handle_append_entries(
        state,
        log,
        AppendEntriesRequest(
            term=1,
            leader_id="node-1",
            prev_log_index=5,
            prev_log_term=1,
        ),
    )

    assert response.success is False

def test_rejects_previous_term_mismatch():
    state = RaftState(node_id="node-2")
    log = RaftLog()

    log.append(
        term=1,
        command=RaftCommand(
            operation="PUT",
            key="a",
            value="1",
        ),
    )

    response = handle_append_entries(
        state,
        log,
        AppendEntriesRequest(
            term=2,
            leader_id="node-1",
            prev_log_index=1,
            prev_log_term=99,
        ),
    )

    assert response.success is False

def test_appends_new_entries():
    state = RaftState(node_id="node-2")
    log = RaftLog()

    response = handle_append_entries(
        state,
        log,
        AppendEntriesRequest(
            term=1,
            leader_id="node-1",
            entries=(
                entry(1, 1, "a"),
                entry(2, 1, "b"),
            ),
        ),
    )

    assert response.success is True
    assert log.last_index == 2
    assert log.get(2) == entry(2, 1, "b")


def test_conflicting_suffix_is_replaced():
    state = RaftState(node_id="node-2")
    log = RaftLog()

    log.append_entry(entry(1, 1, "a"))
    log.append_entry(entry(2, 1, "b"))
    log.append_entry(entry(3, 4, "wrong"))
    log.append_entry(entry(4, 4, "wrong-again"))

    response = handle_append_entries(
        state,
        log,
        AppendEntriesRequest(
            term=5,
            leader_id="node-1",
            prev_log_index=2,
            prev_log_term=1,
            entries=(
                entry(3, 2, "c"),
                entry(4, 2, "d"),
            ),
        ),
    )

    assert response.success is True

    assert log.last_index == 4
    assert log.get(3) == entry(3, 2, "c")
    assert log.get(4) == entry(4, 2, "d")

def test_matching_existing_entry_is_not_duplicated():
    state = RaftState(node_id="node-2")
    log = RaftLog()

    log.append_entry(entry(1, 1, "a"))

    response = handle_append_entries(
        state,
        log,
        AppendEntriesRequest(
            term=1,
            leader_id="node-1",
            entries=(
                entry(1, 1, "a"),
            ),
        ),
    )

    assert response.success is True
    assert log.last_index == 1