from pyraftkv.storage.engine import StorageEngine


def test_snapshot_survives_restart(tmp_path):
    wal_path = tmp_path / "wal.log"
    snapshot_path = tmp_path / "snapshot.json"

    engine = StorageEngine(wal_path, snapshot_path)

    engine.put("name", "harsha")
    engine.put("language", "python")

    engine.create_snapshot()

    restarted = StorageEngine(wal_path, snapshot_path)

    assert restarted.get("name") == "harsha"
    assert restarted.get("language") == "python"


def test_snapshot_truncates_wal(tmp_path):
    wal_path = tmp_path / "wal.log"
    snapshot_path = tmp_path / "snapshot.json"

    engine = StorageEngine(wal_path, snapshot_path)

    engine.put("a", "1")
    engine.put("b", "2")

    assert wal_path.stat().st_size > 0

    engine.create_snapshot()

    assert wal_path.read_text(encoding="utf-8") == ""


def test_operations_after_snapshot_are_replayed(tmp_path):
    wal_path = tmp_path / "wal.log"
    snapshot_path = tmp_path / "snapshot.json"

    engine = StorageEngine(wal_path, snapshot_path)

    engine.put("language", "java")
    engine.create_snapshot()

    engine.put("language", "python")
    engine.put("name", "harsha")

    restarted = StorageEngine(wal_path, snapshot_path)

    assert restarted.get("language") == "python"
    assert restarted.get("name") == "harsha"
