from pathlib import Path

from pyraftkv.storage.store import KVStore
from pyraftkv.storage.wal import WAL


class StorageEngine:
    def __init__(self, wal_path: str | Path) -> None:
        self.store = KVStore()
        self.wal = WAL(wal_path)

        self._recover()

    def _recover(self) -> None:
        for entry in self.wal.replay():
            self.store.apply(entry)

    def put(self, key: str, value: str) -> None:
        self.wal.append_put(key, value)
        self.store.put(key, value)

    def get(self, key: str) -> str | None:
        return self.store.get(key)

    def delete(self, key: str) -> bool:
        if self.store.get(key) is None:
            return False

        self.wal.append_delete(key)
        return self.store.delete(key)
