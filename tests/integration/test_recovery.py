from pyraftkv.storage.store import KVStore
from pyraftkv.storage.wal import WAL


def test_store_recovers_from_wal(tmp_path):
    wal_path = tmp_path / "wal.log"
    wal = WAL(wal_path)

    wal.append_put("name", "harsha")
    wal.append_put("language", "python")
    wal.append_delete("name")

    recovered_store = KVStore()

    for entry in wal.replay():
        recovered_store.apply(entry)

    assert recovered_store.get("name") is None
    assert recovered_store.get("language") == "python"
