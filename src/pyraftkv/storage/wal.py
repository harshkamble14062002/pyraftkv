import json
from pathlib import Path


class WAL:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append_put(self, key: str, value: str) -> None:
        entry = {
            "operation": "PUT",
            "key": key,
            "value": value,
        }
        self._append(entry)

    def append_delete(self, key: str) -> None:
        entry = {
            "operation": "DELETE",
            "key": key,
            "value": None,
        }
        self._append(entry)

    def _append(self, entry: dict[str, str | None]) -> None:
        with self.path.open("a", encoding="utf-8") as file:
            file.write(json.dumps(entry) + "\n")



    def replay(self) -> list[dict[str, str | None]]:
        if not self.path.exists():
            return []

        entries: list[dict[str, str | None]] = []

        with self.path.open("r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()

                if not line:
                    continue

                entries.append(json.loads(line))

        return entries
