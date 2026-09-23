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
    def __init__(
        self,
        base_index: int = 0,
        base_term: int = 0,
        entries: list[LogEntry] | None = None,
    ) -> None:
        if base_index < 0:
            raise ValueError("base_index cannot be negative")

        if base_term < 0:
            raise ValueError("base_term cannot be negative")

        if base_index == 0 and base_term != 0:
            raise ValueError("base_term must be zero when base_index is zero")

        self._base_index = base_index
        self._base_term = base_term
        self._entries: list[LogEntry] = []

        for entry in entries or []:
            self.append_entry(entry)

    @property
    def base_index(self) -> int:
        return self._base_index

    @property
    def base_term(self) -> int:
        return self._base_term

    @property
    def first_index(self) -> int:
        return self._base_index + 1

    @property
    def last_index(self) -> int:
        if not self._entries:
            return self._base_index

        return self._entries[-1].index

    @property
    def last_term(self) -> int:
        if not self._entries:
            return self._base_term

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
        if (
            index <= self._base_index
            or index > self.last_index
        ):
            return None

        position = index - self._base_index - 1

        return self._entries[position]

    def term_at(self, index: int) -> int | None:
        if index == self._base_index:
            return self._base_term

        entry = self.get(index)

        if entry is None:
            return None

        return entry.term

    def truncate_from(self, index: int) -> None:
        if index <= self._base_index:
            raise ValueError(
                "cannot truncate at or before snapshot boundary"
            )

        position = index - self._base_index - 1

        if position < len(self._entries):
            self._entries = self._entries[:position]

    def entries_from(self, index: int) -> list[LogEntry]:
        if index <= 0:
            raise ValueError("index must be greater than zero")

        retained_index = max(
            index,
            self.first_index,
        )
        position = retained_index - self._base_index - 1

        return self._entries[position:].copy()

    def append_entry(self, entry: LogEntry) -> None:
        expected_index = self.last_index + 1

        if entry.index != expected_index:
            raise ValueError(
                f"Expected log index {expected_index}, "
                f"got {entry.index}"
            )

        self._entries.append(entry)

    def compact_through(self, index: int) -> None:
        if index < self._base_index:
            raise ValueError(
                "cannot compact before snapshot boundary"
            )

        if index == self._base_index:
            return

        term = self.term_at(index)

        if term is None:
            raise ValueError(
                f"Cannot compact missing log index {index}"
            )

        retained = self.entries_from(index + 1)
        self._base_index = index
        self._base_term = term
        self._entries = retained

    def install_snapshot_boundary(
        self,
        index: int,
        term: int,
    ) -> None:
        if index < self._base_index:
            raise ValueError(
                "cannot install an older snapshot boundary"
            )

        if index == self._base_index:
            if term != self._base_term:
                raise ValueError(
                    "snapshot term conflicts with current boundary"
                )
            return

        if self.term_at(index) == term:
            retained = self.entries_from(index + 1)
        else:
            retained = []

        self._base_index = index
        self._base_term = term
        self._entries = retained

    def copy(self) -> "RaftLog":
        return RaftLog(
            base_index=self._base_index,
            base_term=self._base_term,
            entries=self._entries.copy(),
        )
