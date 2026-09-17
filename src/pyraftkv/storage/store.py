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


    def apply(self, entry: dict[str, str | None]) -> None:
        operation = entry["operation"]
        key = entry["key"]

        if operation == "PUT":
            value = entry["value"]

            if value is None:
                raise ValueError("PUT operation requires a value")

            self.put(key, value)

        elif operation == "DELETE":
            self.delete(key)

        else:
            raise ValueError(f"Unknown operation: {operation}")