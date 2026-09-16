class KVStore:
    def __init__(self) -> None:
        self._data: dict[str, str] = {}

    def put(self, key: str, value: str) -> None:
        self._data[key] = value

    def get(self, key: str) -> str | None:
        return self._data.get(key)

    def delete(self, key: str) -> bool:
        if key not in self._data:
            return False

        del self._data[key]
        return True

    def clear(self) -> None:
        self._data.clear()