from pyraftkv.storage.engine import StorageEngine


def test_put_survives_restart(tmp_path):
    wal_path = tmp_path / "wal.log"

    engine = StorageEngine(wal_path)
    engine.put("name", "harsha")

    restarted_engine = StorageEngine(wal_path)

    assert restarted_engine.get("name") == "harsha"


def test_delete_survives_restart(tmp_path):
    wal_path = tmp_path / "wal.log"

    engine = StorageEngine(wal_path)
    engine.put("name", "harsha")
    engine.delete("name")

    restarted_engine = StorageEngine(wal_path)

    assert restarted_engine.get("name") is None


def test_multiple_operations_recover_in_order(tmp_path):
    wal_path = tmp_path / "wal.log"

    engine = StorageEngine(wal_path)

    engine.put("language", "java")
    engine.put("language", "python")
    engine.put("name", "harsha")
    engine.delete("name")

    restarted_engine = StorageEngine(wal_path)

    assert restarted_engine.get("language") == "python"
    assert restarted_engine.get("name") is None
