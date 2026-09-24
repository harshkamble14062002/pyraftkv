import json
from unittest.mock import patch

import pytest

from pyraftkv.raft.log import RaftCommand, RaftLog
from pyraftkv.raft.persistence import RaftPersistence
from pyraftkv.raft.snapshot import RaftSnapshot
from pyraftkv.transport.serialization import log_entry_to_dict


def test_missing_snapshot_defaults_to_empty_boundary(
    tmp_path,
):
    persistence = RaftPersistence(tmp_path)

    assert persistence.load_snapshot() == RaftSnapshot()


def test_snapshot_save_load_and_exact_format(tmp_path):
    persistence = RaftPersistence(tmp_path)
    snapshot = RaftSnapshot(
        last_included_index=12,
        last_included_term=4,
        state={
            "language": "python",
        },
    )

    persistence.save_snapshot(snapshot)

    assert persistence.load_snapshot() == snapshot
    assert json.loads(
        persistence.snapshot_path.read_text(
            encoding="utf-8"
        )
    ) == {
        "version": 1,
        "last_included_index": 12,
        "last_included_term": 4,
        "state": {
            "language": "python",
        },
    }


def test_failed_snapshot_replacement_keeps_previous_file(
    tmp_path,
):
    persistence = RaftPersistence(tmp_path)
    original = RaftSnapshot(
        last_included_index=2,
        last_included_term=1,
        state={"key": "old"},
    )
    persistence.save_snapshot(original)

    with (
        patch(
            "pyraftkv.raft.persistence.json.dump",
            side_effect=OSError("write failed"),
        ),
        pytest.raises(OSError),
    ):
        persistence.save_snapshot(
            RaftSnapshot(
                last_included_index=3,
                last_included_term=2,
                state={"key": "new"},
            )
        )

    assert persistence.load_snapshot() == original
    assert not list(tmp_path.glob(".raft-snapshot.json.*.tmp"))


def test_compacted_log_round_trip_preserves_boundary(
    tmp_path,
):
    persistence = RaftPersistence(tmp_path)
    log = RaftLog()

    for term, key in (
        (1, "one"),
        (1, "two"),
        (2, "three"),
    ):
        log.append(
            term=term,
            command=RaftCommand(
                operation="PUT",
                key=key,
                value=key,
            ),
        )

    log.compact_through(2)
    persistence.save_log(log)

    restored = persistence.load_log()

    assert restored.base_index == 2
    assert restored.base_term == 1
    assert restored.term_at(2) == 1
    assert restored.get(3) == log.get(3)


def test_legacy_journal_snapshot_defaults_to_base_zero(
    tmp_path,
):
    persistence = RaftPersistence(tmp_path)
    log = RaftLog()
    entry = log.append(
        term=1,
        command=RaftCommand(
            operation="PUT",
            key="legacy",
            value="value",
        ),
    )
    persistence.log_path.write_text(
        json.dumps(
            {
                "type": "snapshot",
                "entries": [
                    log_entry_to_dict(entry),
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    restored = persistence.load_log()

    assert restored.base_index == 0
    assert restored.base_term == 0
    assert restored.get(1) == entry


def test_legacy_json_array_log_still_loads(tmp_path):
    persistence = RaftPersistence(tmp_path)
    log = RaftLog()
    entry = log.append(
        term=2,
        command=RaftCommand(
            operation="PUT",
            key="legacy-array",
            value="value",
        ),
    )
    persistence.log_path.write_text(
        json.dumps([log_entry_to_dict(entry)]),
        encoding="utf-8",
    )

    restored = persistence.load_log()

    assert restored.base_index == 0
    assert restored.get(1) == entry
