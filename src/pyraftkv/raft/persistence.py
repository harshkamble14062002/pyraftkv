import json
import os
import tempfile
from pathlib import Path
from threading import RLock
from typing import Any

from pyraftkv.raft.log import RaftLog
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

        self._write_lock = RLock()

    def _atomic_write(
        self,
        path: Path,
        data: Any,
    ) -> None:
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
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (
            json.JSONDecodeError,
            OSError,
        ) as exc:
            raise RaftPersistenceError("Unable to load Raft state") from exc

        return RaftState(
            node_id=node_id,
            current_term=int(data["current_term"]),
            voted_for=data.get("voted_for"),
            commit_index=int(data.get("commit_index", 0)),
        )

    def save_log(
        self,
        log: RaftLog,
    ) -> None:
        entries = [log_entry_to_dict(entry) for entry in log.entries_from(1)]

        self._atomic_write(
            self.log_path,
            entries,
        )

    def load_log(self) -> RaftLog:
        log = RaftLog()

        if not self.log_path.exists():
            return log

        try:
            raw_entries = json.loads(self.log_path.read_text(encoding="utf-8"))
        except (
            json.JSONDecodeError,
            OSError,
        ) as exc:
            raise RaftPersistenceError("Unable to load Raft log") from exc

        if not isinstance(
            raw_entries,
            list,
        ):
            raise RaftPersistenceError("Raft log must contain a list")

        for raw_entry in raw_entries:
            entry = log_entry_from_dict(raw_entry)

            log.append_entry(entry)

        return log
