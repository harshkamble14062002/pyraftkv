import json

from pyraftkv.storage.wal import WAL


def test_append_put(tmp_path):
    wal_path = tmp_path / "wal.log"
    wal = WAL(wal_path)

    wal.append_put("name", "harsha")

    lines = wal_path.read_text().splitlines()

    assert len(lines) == 1

    entry = json.loads(lines[0])

    assert entry == {
        "operation": "PUT",
        "key": "name",
        "value": "harsha",
    }


def test_append_delete(tmp_path):
    wal_path = tmp_path / "wal.log"
    wal = WAL(wal_path)

    wal.append_delete("name")

    entry = json.loads(wal_path.read_text().strip())

    assert entry == {
        "operation": "DELETE",
        "key": "name",
        "value": None,
    }


def test_multiple_entries_preserve_order(tmp_path):
    wal_path = tmp_path / "wal.log"
    wal = WAL(wal_path)

    wal.append_put("name", "harsha")
    wal.append_put("language", "python")
    wal.append_delete("name")

    lines = wal_path.read_text().splitlines()

    entries = [json.loads(line) for line in lines]

    assert entries == [
        {
            "operation": "PUT",
            "key": "name",
            "value": "harsha",
        },
        {
            "operation": "PUT",
            "key": "language",
            "value": "python",
        },
        {
            "operation": "DELETE",
            "key": "name",
            "value": None,
        },
    ]


def test_existing_entries_are_not_overwritten(tmp_path):
    wal_path = tmp_path / "wal.log"

    wal1 = WAL(wal_path)
    wal1.append_put("a", "1")

    wal2 = WAL(wal_path)
    wal2.append_put("b", "2")

    lines = wal_path.read_text().splitlines()

    assert len(lines) == 2

def test_replay_entries(tmp_path):
    wal_path = tmp_path / "wal.log"
    wal = WAL(wal_path)

    wal.append_put("name", "harsha")
    wal.append_put("language", "python")
    wal.append_delete("name")

    entries = wal.replay()

    assert entries == [
        {
            "operation": "PUT",
            "key": "name",
            "value": "harsha",
        },
        {
            "operation": "PUT",
            "key": "language",
            "value": "python",
        },
        {
            "operation": "DELETE",
            "key": "name",
            "value": None,
        },
    ]