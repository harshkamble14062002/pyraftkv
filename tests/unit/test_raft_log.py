import pytest

from pyraftkv.raft.log import RaftCommand, RaftLog


def test_empty_log_has_zero_index_and_term():
    log = RaftLog()

    assert log.last_index == 0
    assert log.last_term == 0


def test_append_creates_first_entry():
    log = RaftLog()

    command = RaftCommand(
        operation="PUT",
        key="name",
        value="harsha",
    )

    entry = log.append(
        term=1,
        command=command,
    )

    assert entry.index == 1
    assert entry.term == 1
    assert entry.command == command

    assert log.last_index == 1
    assert log.last_term == 1


def test_entries_receive_sequential_indexes():
    log = RaftLog()

    first = log.append(
        term=1,
        command=RaftCommand(
            operation="PUT",
            key="a",
            value="1",
        ),
    )

    second = log.append(
        term=1,
        command=RaftCommand(
            operation="PUT",
            key="b",
            value="2",
        ),
    )

    assert first.index == 1
    assert second.index == 2
    assert log.last_index == 2


def test_get_entry_by_index():
    log = RaftLog()

    log.append(
        term=2,
        command=RaftCommand(
            operation="PUT",
            key="language",
            value="python",
        ),
    )

    entry = log.get(1)

    assert entry is not None
    assert entry.term == 2
    assert entry.command.key == "language"


def test_missing_index_returns_none():
    log = RaftLog()

    assert log.get(1) is None
    assert log.get(0) is None


def test_term_at_zero_is_zero():
    log = RaftLog()

    assert log.term_at(0) == 0


def test_truncate_conflicting_suffix():
    log = RaftLog()

    for value in ("1", "2", "3", "4"):
        log.append(
            term=1,
            command=RaftCommand(
                operation="PUT",
                key=value,
                value=value,
            ),
        )

    log.truncate_from(3)

    assert log.last_index == 2
    assert log.get(3) is None
    assert log.get(4) is None


def test_entries_from_index():
    log = RaftLog()

    for value in ("1", "2", "3"):
        log.append(
            term=1,
            command=RaftCommand(
                operation="PUT",
                key=value,
                value=value,
            ),
        )

    entries = log.entries_from(2)

    assert [entry.index for entry in entries] == [2, 3]


def test_negative_term_is_rejected():
    log = RaftLog()

    with pytest.raises(ValueError):
        log.append(
            term=-1,
            command=RaftCommand(
                operation="PUT",
                key="x",
                value="10",
            ),
        )


def make_compacted_log() -> RaftLog:
    log = RaftLog()

    for index, term in enumerate(
        (1, 1, 2, 2),
        start=1,
    ):
        log.append(
            term=term,
            command=RaftCommand(
                operation="PUT",
                key=str(index),
                value=str(index),
            ),
        )

    log.compact_through(2)

    return log


def test_base_zero_behavior_remains_compatible():
    log = RaftLog()

    assert log.base_index == 0
    assert log.base_term == 0
    assert log.first_index == 1
    assert log.term_at(0) == 0


def test_non_zero_base_index_tracks_snapshot_boundary():
    log = make_compacted_log()

    assert log.base_index == 2
    assert log.base_term == 1
    assert log.first_index == 3
    assert log.last_index == 4


def test_term_at_snapshot_boundary_after_compaction():
    log = make_compacted_log()

    assert log.term_at(2) == 1
    assert log.term_at(1) is None
    assert log.term_at(3) == 2


def test_get_and_entries_from_after_compaction():
    log = make_compacted_log()

    assert log.get(2) is None
    assert log.get(3) is not None
    assert [
        entry.index
        for entry in log.entries_from(1)
    ] == [3, 4]
    assert [
        entry.index
        for entry in log.entries_from(4)
    ] == [4]


def test_truncate_from_after_compaction():
    log = make_compacted_log()

    log.truncate_from(4)

    assert log.last_index == 3
    assert log.get(4) is None

    with pytest.raises(ValueError):
        log.truncate_from(2)


def test_append_after_compaction_uses_logical_index():
    log = make_compacted_log()
    log.compact_through(4)

    entry = log.append(
        term=3,
        command=RaftCommand(
            operation="PUT",
            key="5",
            value="5",
        ),
    )

    assert entry.index == 5
    assert log.get(5) == entry


def test_matching_snapshot_boundary_preserves_suffix():
    log = make_compacted_log()

    log.install_snapshot_boundary(
        index=3,
        term=2,
    )

    assert log.base_index == 3
    assert log.get(4) is not None


def test_conflicting_snapshot_boundary_discards_suffix():
    log = make_compacted_log()

    log.install_snapshot_boundary(
        index=3,
        term=9,
    )

    assert log.base_index == 3
    assert log.base_term == 9
    assert log.last_index == 3
