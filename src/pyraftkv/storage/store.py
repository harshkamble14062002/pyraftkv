from threading import RLock


class KVStore:
    def __init__(self) -> None:
        self._data: dict[str, str] = {}
        self._lock = RLock()

    def put(self, key: str, value: str) -> None:
        with self._lock:
            self._data[key] = value

    def get(self, key: str) -> str | None:
        with self._lock:
            return self._data.get(key)

    def delete(self, key: str) -> bool:
        with self._lock:
            if key not in self._data:
                return False

            del self._data[key]
            return True

    def clear(self) -> None:
        with self._lock:
            self._data.clear()