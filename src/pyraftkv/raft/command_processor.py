import time
from concurrent.futures import Future
from dataclasses import dataclass
from queue import Empty, Full, Queue
from threading import Lock, Thread

from pyraftkv.raft.log import RaftCommand
from pyraftkv.raft.node import RaftNode
from pyraftkv.transport.base import RaftTransport


class CommandQueueFullError(RuntimeError):
    """Raised when the bounded Raft command queue is full."""


@dataclass
class _QueuedCommand:
    command: RaftCommand
    future: Future[bool]


class RaftCommandProcessor:
    def __init__(
        self,
        node: RaftNode,
        transport: RaftTransport,
        max_queue_size: int = 1024,
        max_batch_size: int = 64,
        batch_wait_seconds: float = 0.002,
    ) -> None:
        self.node = node
        self.transport = transport
        self.max_batch_size = max_batch_size
        self.batch_wait_seconds = batch_wait_seconds
        self._queue: Queue[_QueuedCommand | None] = Queue(
            maxsize=max_queue_size,
        )
        self._thread: Thread | None = None
        self._lifecycle_lock = Lock()

    def start(self) -> None:
        with self._lifecycle_lock:
            if self._thread is not None:
                return

            self._thread = Thread(
                target=self._run,
                name=f"raft-writer-{self.node.node_id}",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        with self._lifecycle_lock:
            thread = self._thread
            if thread is None:
                return

            self._thread = None
            self._queue.put(None)

        thread.join()

    def submit(self, command: RaftCommand) -> bool:
        future: Future[bool] = Future()

        with self._lifecycle_lock:
            if self._thread is None:
                raise RuntimeError("Raft command processor is not running")

            try:
                self._queue.put_nowait(
                    _QueuedCommand(
                        command=command,
                        future=future,
                    )
                )
            except Full as exc:
                raise CommandQueueFullError("Raft command queue is full") from exc

        return future.result()

    def _collect_batch(
        self,
        first: _QueuedCommand,
    ) -> tuple[list[_QueuedCommand], bool]:
        batch = [first]
        deadline = time.monotonic() + self.batch_wait_seconds
        stopping = False

        while len(batch) < self.max_batch_size:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break

            try:
                item = self._queue.get(timeout=remaining)
            except Empty:
                break

            if item is None:
                stopping = True
                break

            batch.append(item)

        return batch, stopping

    def _run(self) -> None:
        stopping = False

        while not stopping:
            item = self._queue.get()
            if item is None:
                return

            batch, stopping = self._collect_batch(item)

            try:
                results = self.node.submit_commands(
                    [queued.command for queued in batch],
                    self.transport,
                )
            except Exception as exc:  # noqa: BLE001
                for queued in batch:
                    queued.future.set_exception(exc)
            else:
                for queued, result in zip(batch, results, strict=True):
                    queued.future.set_result(result)
