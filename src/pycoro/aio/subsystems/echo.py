from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from queue import Full, Queue, ShutDown
from threading import Thread
from typing import TYPE_CHECKING

from pycoro import CQE
from pycoro.bus import SQE

if TYPE_CHECKING:
    from pycoro.aio import AIO


# Submission
@dataclass(frozen=True)
class EchoSubmission:
    data: str

    @property
    def kind(self) -> str:
        return "echo"


# Completion
@dataclass(frozen=True)
class EchoCompletion:
    data: str

    @property
    def kind(self) -> str:
        return "echo"


class EchoSubsystem:
    def __init__(
        self,
        aio: AIO[EchoSubmission, EchoCompletion],
        size: int = 100,
        workers: int = 1,
    ) -> None:
        self._aio = aio
        self._sq = Queue[SQE[EchoSubmission, EchoCompletion]](size)
        self._workers = workers
        self._threads: list[Thread] = []

    @property
    def size(self) -> int:
        return self._sq.maxsize

    @property
    def kind(self) -> str:
        return "echo"

    def start(self) -> None:
        assert len(self._threads) == 0

        for i in range(self._workers):
            t = Thread(target=self.worker, daemon=True, name=f"echo-worker-{i}")
            t.start()
            self._threads.append(t)

    def shutdown(self) -> None:
        assert len(self._threads) == self._workers
        self._sq.shutdown()
        for t in self._threads:
            t.join()

        self._threads.clear()
        assert len(self._threads) == 0, "at least one worker must be set."
        self._sq.join()

    def enqueue(self, sqe: SQE) -> bool:
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
        assert not isinstance(sqe.value, Callable)

        return [
            CQE(
                EchoCompletion(sqe.value.data),
                sqe.callback,
            ),
        ]

    def worker(self) -> None:
        while True:
            try:
                sqe = self._sq.get()
            except ShutDown:
                break

            assert not isinstance(sqe.value, Callable)
            assert sqe.value.kind == self.kind

            self._aio.enqueue((self.process([sqe])[0], self.kind))
            self._sq.task_done()
