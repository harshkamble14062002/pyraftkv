import json
import os
import tempfile
from pathlib import Path
from threading import RLock
from typing import Any

from pyraftkv.raft.log import LogEntry, RaftLog
from pyraftkv.raft.snapshot import RaftSnapshot
from pyraftkv.raft.state import RaftState
from pyraftkv.transport.serialization import (
    log_entry_from_dict,
    log_entry_to_dict,
)


class RaftPersistenceError(RuntimeError):
    pass


class RaftPersistence:
    def __init__(
        self,
        directory: str | Path,
    ) -> None:
        self.directory = Path(directory)

        self.directory.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.state_path = self.directory / "raft-state.json"
        self.log_path = self.directory / "raft-log.json"
        self.snapshot_path = self.directory / "raft-snapshot.json"

        self._write_lock = RLock()

    def _atomic_write(
        self,
        path: Path,
        data: Any,
    ) -> None:
        """Atomically replace a JSON file.

        Used for Raft state, where the complete object is small and should
        always be replaced as one atomic unit.
        """
        with self._write_lock:
            fd, temp_name = tempfile.mkstemp(
                prefix=f".{path.name}.",
                suffix=".tmp",
                dir=path.parent,
                text=True,
            )

            temp_path = Path(temp_name)

            try:
                with os.fdopen(
                    fd,
                    "w",
                    encoding="utf-8",
                ) as file:
                    json.dump(
                        data,
                        file,
                        separators=(",", ":"),
                    )

                    file.flush()
                    os.fsync(file.fileno())

                os.replace(
                    temp_path,
                    path,
                )

            except Exception:
                try:
                    temp_path.unlink()
                except FileNotFoundError:
                    pass

                raise

    def save_state(
        self,
        state: RaftState,
    ) -> None:
        self._atomic_write(
            self.state_path,
            {
                "current_term": state.current_term,
                "voted_for": state.voted_for,
                "commit_index": state.commit_index,
            },
        )

    def load_state(
        self,
        node_id: str,
    ) -> RaftState:
        if not self.state_path.exists():
            return RaftState(
                node_id=node_id,
            )

        try:
            data = json.loads(
                self.state_path.read_text(
                    encoding="utf-8",
                )
            )
        except (
            json.JSONDecodeError,
            OSError,
        ) as exc:
            raise RaftPersistenceError(
                "Unable to load Raft state"
            ) from exc

        return RaftState(
            node_id=node_id,
            current_term=int(data["current_term"]),
            voted_for=data.get("voted_for"),
            commit_index=int(data.get("commit_index", 0)),
        )

    def save_snapshot(
        self,
        snapshot: RaftSnapshot,
    ) -> None:
        self._atomic_write(
            self.snapshot_path,
            {
                "version": snapshot.version,
                "last_included_index": (
                    snapshot.last_included_index
                ),
                "last_included_term": (
                    snapshot.last_included_term
                ),
                "state": snapshot.state,
            },
        )

    def load_snapshot(self) -> RaftSnapshot:
        if not self.snapshot_path.exists():
            return RaftSnapshot()

        try:
            data = json.loads(
                self.snapshot_path.read_text(
                    encoding="utf-8",
                )
            )
            raw_state = data["state"]

            if not isinstance(raw_state, dict):
                raise TypeError(
                    "snapshot state must be an object"
                )

            state = {
                str(key): str(value)
                for key, value in raw_state.items()
            }

            return RaftSnapshot(
                version=int(data.get("version", 1)),
                last_included_index=int(
                    data.get("last_included_index", 0)
                ),
                last_included_term=int(
                    data.get("last_included_term", 0)
                ),
                state=state,
            )
        except (
            json.JSONDecodeError,
            KeyError,
            OSError,
            TypeError,
            ValueError,
        ) as exc:
            raise RaftPersistenceError(
                "Unable to load Raft snapshot"
            ) from exc

    def _rewrite_log_records(
        self,
        records: list[dict[str, Any]],
    ) -> None:
        """Atomically replace the persistent Raft log journal."""
        with self._write_lock:
            fd, temp_name = tempfile.mkstemp(
                prefix=f".{self.log_path.name}.",
                suffix=".tmp",
                dir=self.log_path.parent,
                text=True,
            )

            temp_path = Path(temp_name)

            try:
                with os.fdopen(
                    fd,
                    "w",
                    encoding="utf-8",
                ) as file:
                    for record in records:
                        file.write(
                            json.dumps(
                                record,
                                separators=(",", ":"),
                            )
                        )
                        file.write("\n")

                    file.flush()
                    os.fsync(file.fileno())

                os.replace(
                    temp_path,
                    self.log_path,
                )

            except Exception:
                try:
                    temp_path.unlink()
                except FileNotFoundError:
                    pass

                raise

    def _append_log_records(
        self,
        records: list[dict[str, Any]],
    ) -> None:
        """Durably append one or more records to the Raft log journal."""
        if not records:
            return

        with self._write_lock:
            try:
                with self.log_path.open(
                    "a",
                    encoding="utf-8",
                ) as file:
                    for record in records:
                        file.write(
                            json.dumps(
                                record,
                                separators=(",", ":"),
                            )
                        )
                        file.write("\n")

                    file.flush()
                    os.fsync(file.fileno())

            except OSError as exc:
                raise RaftPersistenceError(
                    "Unable to persist Raft log"
                ) from exc

    def append_log_entries(
        self,
        entries: list[LogEntry],
    ) -> None:
        """Persist newly appended Raft entries without rewriting the log."""
        records = [
            {
                "type": "append",
                "entry": log_entry_to_dict(entry),
            }
            for entry in entries
        ]

        self._append_log_records(
            records
        )

    def truncate_log_from(
        self,
        index: int,
    ) -> None:
        """Persist removal of all entries beginning at index."""
        if index <= 0:
            raise ValueError(
                "index must be greater than zero"
            )

        self._append_log_records(
            [
                {
                    "type": "truncate",
                    "from_index": index,
                }
            ]
        )

    def replace_log_suffix(
        self,
        from_index: int,
        entries: list[LogEntry],
    ) -> None:
        if from_index <= 0:
            raise ValueError(
                "from_index must be greater than zero"
            )

        records = [
            {
                "type": "truncate",
                "from_index": from_index,
            },
            *[
                {
                    "type": "append",
                    "entry": log_entry_to_dict(entry),
                }
                for entry in entries
            ],
        ]

        self._append_log_records(records)

    def save_log(
        self,
        log: RaftLog,
    ) -> None:
        """Rewrite the complete log as a journal snapshot.

        This remains available for migration, recovery, and fallback paths.
        Normal Raft appends should use append_log_entries() instead.
        """
        entries = [
            log_entry_to_dict(entry)
            for entry in log.entries_from(log.first_index)
        ]

        self._rewrite_log_records(
            [
                {
                    "type": "snapshot",
                    "version": 1,
                    "base_index": log.base_index,
                    "base_term": log.base_term,
                    "entries": entries,
                }
            ]
        )

    def load_log(self) -> RaftLog:
        if not self.log_path.exists():
            return RaftLog()

        try:
            content = self.log_path.read_text(
                encoding="utf-8",
            )
        except OSError as exc:
            raise RaftPersistenceError(
                "Unable to load Raft log"
            ) from exc

        if not content.strip():
            return RaftLog()

        # Backward compatibility with the previous representation:
        #
        # [
        #     {"index": 1, ...},
        #     {"index": 2, ...}
        # ]
        #
        # If an old-format log is found, load it and immediately migrate it
        # to the new journal representation.
        try:
            legacy_data = json.loads(
                content
            )
        except json.JSONDecodeError:
            legacy_data = None

        if isinstance(
            legacy_data,
            list,
        ):
            log = RaftLog()

            try:
                for raw_entry in legacy_data:
                    entry = log_entry_from_dict(
                        raw_entry
                    )
                    log.append_entry(
                        entry
                    )

            except (
                KeyError,
                TypeError,
                ValueError,
            ) as exc:
                raise RaftPersistenceError(
                    "Unable to load Raft log"
                ) from exc

            # One-time migration from the old JSON-array representation.
            self.save_log(
                log
            )

            return log

        log = RaftLog()

        try:
            for line_number, line in enumerate(
                content.splitlines(),
                start=1,
            ):
                if not line.strip():
                    continue

                record = json.loads(
                    line
                )

                if not isinstance(
                    record,
                    dict,
                ):
                    raise RaftPersistenceError(
                        "Invalid Raft log record "
                        f"at line {line_number}"
                    )

                record_type = record.get(
                    "type"
                )

                if record_type == "snapshot":
                    raw_entries = record.get(
                        "entries"
                    )

                    if not isinstance(
                        raw_entries,
                        list,
                    ):
                        raise RaftPersistenceError(
                            "Invalid Raft log snapshot "
                            f"at line {line_number}"
                        )

                    log = RaftLog(
                        base_index=int(
                            record.get("base_index", 0)
                        ),
                        base_term=int(record.get("base_term", 0)),
                    )

                    for raw_entry in raw_entries:
                        entry = log_entry_from_dict(
                            raw_entry
                        )

                        log.append_entry(
                            entry
                        )

                elif record_type == "append":
                    raw_entry = record.get(
                        "entry"
                    )

                    if not isinstance(
                        raw_entry,
                        dict,
                    ):
                        raise RaftPersistenceError(
                            "Invalid Raft append record "
                            f"at line {line_number}"
                        )

                    entry = log_entry_from_dict(
                        raw_entry
                    )

                    log.append_entry(
                        entry
                    )

                elif record_type == "truncate":
                    index = int(
                        record["from_index"]
                    )

                    log.truncate_from(
                        index
                    )

                else:
                    raise RaftPersistenceError(
                        "Unknown Raft log record type "
                        f"at line {line_number}"
                    )

        except RaftPersistenceError:
            raise

        except (
            json.JSONDecodeError,
            KeyError,
            TypeError,
            ValueError,
        ) as exc:
            raise RaftPersistenceError(
                "Unable to load Raft log"
            ) from exc

        return log