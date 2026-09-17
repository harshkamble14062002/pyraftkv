from dataclasses import dataclass


@dataclass(frozen=True)
class RaftCommand:
    operation: str
    key: str
    value: str | None = None


@dataclass(frozen=True)
class LogEntry:
    index: int
    term: int
    command: RaftCommand


class RaftLog:
    def __init__(self) -> None:
        self._entries: list[LogEntry] = []

    @property
    def last_index(self) -> int:
        if not self._entries:
            return 0

        return self._entries[-1].index

    @property
    def last_term(self) -> int:
        if not self._entries:
            return 0

        return self._entries[-1].term

    def append(
        self,
        term: int,
        command: RaftCommand,
    ) -> LogEntry:
        if term < 0:
            raise ValueError("term cannot be negative")

        entry = LogEntry(
            index=self.last_index + 1,
            term=term,
            command=command,
        )

        self._entries.append(entry)

        return entry

    def get(self, index: int) -> LogEntry | None:
        if index <= 0:
            return None

        if index > self.last_index:
            return None

        return self._entries[index - 1]

    def term_at(self, index: int) -> int | None:
        if index == 0:
            return 0

        entry = self.get(index)

        if entry is None:
            return None

        return entry.term

    def truncate_from(self, index: int) -> None:
        if index <= 0:
            raise ValueError("index must be greater than zero")

        self._entries = [
            entry
            for entry in self._entries
            if entry.index < index
        ]

    def entries_from(self, index: int) -> list[LogEntry]:
        if index <= 0:
            raise ValueError("index must be greater than zero")

        return [
            entry
            for entry in self._entries
            if entry.index >= index
        ]

    def append_entry(self, entry: LogEntry) -> None:
        expected_index = self.last_index + 1

        if entry.index != expected_index:
            raise ValueError(
                f"Expected log index {expected_index}, got {entry.index}"
            )

        self._entries.append(entry)