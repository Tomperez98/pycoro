from __future__ import annotations

from collections.abc import Callable
from queue import Full, Queue, ShutDown
from threading import Thread
from typing import TYPE_CHECKING, Any

from pycoro.bus import CQE, SQE

if TYPE_CHECKING:
    from pycoro.aio import AIO


class FunctionSubsystem:
    def __init__(self, aio: AIO, size: int = 100, workers: int = 1) -> None:
        self._aio = aio
        self._sq = Queue[SQE[Callable[[], Any], Any]](size)
        self._workers = workers
        self._threads: list[Thread] = []

    def size(self) -> int:
        return self._sq.maxsize

    def kind(self) -> str:
        return "function"

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

    def enqueue[T](self, sqe: SQE[Callable[[], T], T]) -> bool:
        assert isinstance(sqe.v, Callable), f"Expected Callable, got {type(sqe.v).__name__}"

        try:
            self._sq.put_nowait(sqe)
        except Full:
            return False
        return True

    def flush(self, time: int) -> None:
        return

    def process[T](self, sqes: list[SQE[Callable[[], T], T]]) -> list[CQE[T]]:
        assert self._workers > 0, "must be at least one worker"
        sqe = sqes[0]
        assert isinstance(sqe.v, Callable), f"Expected Callable, got {type(sqe.v).__name__}"

        return [CQE(sqe.v(), sqe.cb)]

    def worker(self) -> None:
        while True:
            try:
                sqe = self._sq.get()
            except ShutDown:
                break

            assert isinstance(sqe.v, Callable), f"Expected Callable, got {type(sqe.v).__name__}"

            self._aio.enqueue((self.process([sqe])[0], self.kind()))
            self._sq.task_done()
