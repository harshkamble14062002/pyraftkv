from pathlib import Path

from pyraftkv.storage.snapshot import SnapshotStore
from pyraftkv.storage.store import KVStore
from pyraftkv.storage.wal import WAL


class StorageEngine:
    def __init__(
        self,
        wal_path: str | Path,
        snapshot_path: str | Path | None = None,
    ) -> None:
        self.store = KVStore()
        self.wal = WAL(wal_path)

        if snapshot_path is None:
            snapshot_path = Path(wal_path).with_suffix(".snapshot")

        self.snapshot_store = SnapshotStore(snapshot_path)

        self._recover()

    def _recover(self) -> None:
        # 1. Restore the older state from the snapshot.
        snapshot = self.snapshot_store.load()

        if snapshot:
            self.store.restore(snapshot)

        # 2. Apply operations that happened after the snapshot.
        for entry in self.wal.replay():
            self.store.apply(entry)

    def put(self, key: str, value: str) -> None:
        # WAL must be persisted before changing memory.
        self.wal.append_put(key, value)
        self.store.put(key, value)

    def get(self, key: str) -> str | None:
        return self.store.get(key)

    def delete(self, key: str) -> bool:
        if self.store.get(key) is None:
            return False

        # WAL must be persisted before changing memory.
        self.wal.append_delete(key)
        return self.store.delete(key)

    def create_snapshot(self) -> None:
        # Capture the current in-memory state.
        data = self.store.snapshot()

        # SnapshotStore.save() should safely persist the snapshot first.
        self.snapshot_store.save(data)

        # Only truncate WAL after snapshot save succeeds.
        self.wal.truncate()
