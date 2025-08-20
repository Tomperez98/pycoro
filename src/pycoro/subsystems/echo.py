from __future__ import annotations

from dataclasses import dataclass
from queue import Full, Queue, ShutDown
from threading import Thread
from typing import TYPE_CHECKING

from pycoro.bus import CQE, SQE

if TYPE_CHECKING:
    from pycoro.aio import AIO


# Submission
@dataclass(frozen=True)
class EchoSubmission:
    data: str

    def kind(self) -> str:
        return "echo"


# Completion
@dataclass(frozen=True)
class EchoCompletion:
    data: str

    def kind(self) -> str:
        return "echo"


class EchoSubsystem:
    def __init__(
        self,
        aio: AIO,
        size: int = 100,
        workers: int = 1,
    ) -> None:
        self._aio = aio
        self._sq = Queue[SQE[EchoSubmission, EchoCompletion]](size)
        self._workers = workers
        self._threads: list[Thread] = []

    def size(self) -> int:
        return self._sq.maxsize

    def kind(self) -> str:
        return "echo"

    def start(self) -> None:
        assert len(self._threads) == 0, f"Threads already running: {len(self._threads)}"
        for _ in range(self._workers):
            t = Thread(target=self.worker, daemon=True)
            t.start()
            self._threads.append(t)

    def shutdown(self) -> None:
        assert len(self._threads) == self._workers, (
            f"Thread count mismatch: {len(self._threads)} active, expected {self._workers}"
        )
        self._sq.shutdown()
        for t in self._threads:
            t.join()

        self._threads.clear()
        self._sq.join()

    def enqueue(self, sqe: SQE[EchoSubmission, EchoCompletion]) -> bool:
        assert isinstance(sqe.v, EchoSubmission), (
            f"Expected EchoSubmission, got {type(sqe.v).__name__}"
        )
        assert sqe.v.kind() == self.kind(), (
            f"Kind mismatch: sqe.v.kind()={sqe.v.kind()} != self.kind()={self.kind()}"
        )

        try:
            self._sq.put_nowait(sqe)
        except Full:
            return False
        return True

    def flush(self, time: int) -> None:
        return

    def process(self, sqes: list[SQE[EchoSubmission, EchoCompletion]]) -> list[CQE[EchoCompletion]]:
        assert self._workers > 0, "must be at least one worker"
        sqe = sqes[0]
        assert isinstance(sqe.v, EchoSubmission), (
            f"Expected EchoSubmission, got {type(sqe.v).__name__}"
        )

        return [
            CQE(
                EchoCompletion(sqe.v.data),
                sqe.cb,
            ),
        ]

    def worker(self) -> None:
        while True:
            try:
                sqe = self._sq.get()
            except ShutDown:
                break

            assert sqe.v.kind() == self.kind(), (
                f"Kind mismatch: sqe.v.kind()={sqe.v.kind()} != self.kind()={self.kind()}"
            )

            self._aio.enqueue((self.process([sqe])[0], self.kind()))
            self._sq.task_done()
