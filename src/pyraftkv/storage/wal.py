import json
import os
from pathlib import Path


class WALCorruptionError(RuntimeError):
    """Raised when a durable WAL record is corrupted."""


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
            file.flush()
            os.fsync(file.fileno())

    def replay(self) -> list[dict[str, str | None]]:
        if not self.path.exists():
            return []

        lines = self.path.read_text(encoding="utf-8").splitlines()
        entries: list[dict[str, str | None]] = []

        for index, line in enumerate(lines):
            if not line.strip():
                continue

            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError as exc:
                # Ignore an incomplete final WAL record.
                if index == len(lines) - 1:
                    break

                # Corruption in the middle of the WAL is an error.
                raise WALCorruptionError(
                    f"Corrupted WAL record at line {index + 1}"
                ) from exc

        return entries

    def truncate(self) -> None:
        with self.path.open("w", encoding="utf-8") as file:
            file.flush()
            os.fsync(file.fileno())