from __future__ import annotations

from collections.abc import Callable
from queue import Full, Queue, ShutDown
from threading import Thread
from typing import TYPE_CHECKING

from pycoro.bus import CQE, SQE

if TYPE_CHECKING:
    from pycoro.aio import AIO


class FunctionSubsystem:
    def __init__(self, aio: AIO, size: int = 100, workers: int = 1) -> None:
        self._aio = aio
        self._sq = Queue[SQE](size)
        self._workers = workers
        self._threads: list[Thread] = []

    @property
    def size(self) -> int:
        return self._sq.maxsize

    @property
    def kind(self) -> str:
        return "function"

    def start(self) -> None:
        assert len(self._threads) == 0

        for i in range(self._workers):
            t = Thread(target=self.worker, daemon=True, name=f"function-worker-{i}")
            t.start()
            self._threads.append(t)

    def shutdown(self) -> None:
        assert len(self._threads) == self._workers
        self._sq.shutdown()
        for t in self._threads:
            t.join()

        self._threads.clear()
        self._sq.join()

    def enqueue(self, sqe: SQE) -> bool:
        try:
            self._sq.put_nowait(sqe)
        except Full:
            return False
        return True

    def flush(self, time: int) -> None:
        return

    def process(self, sqes: list[SQE]) -> list[CQE]:
        assert self._workers > 0, "must be at least one worker"
        sqe = sqes[0]
        assert isinstance(sqe.value, Callable)
        return [CQE(sqe.value(), sqe.callback)]

    def worker(self) -> None:
        while True:
            try:
                sqe = self._sq.get()
            except ShutDown:
                break

            assert isinstance(sqe.value, Callable)

            self._aio.enqueue((self.process([sqe])[0], "function"))
            self._sq.task_done()
