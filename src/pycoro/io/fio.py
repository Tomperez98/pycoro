from __future__ import annotations

from collections.abc import Callable
from queue import Empty, Queue, ShutDown
from threading import Thread
from typing import TYPE_CHECKING, Any

from pycoro.bus import CQE, SQE

if TYPE_CHECKING:
    from collections.abc import Callable


class FIO[I: Callable[[], Any], O]:
    def __init__(self, size: int, workers: int) -> None:
        self._sq = Queue[SQE[I, O]](size)
        self._cq = Queue[CQE[O]](size)
        self._workers = workers
        self._threads: list[Thread] = []

    def dispatch(self, sqe: SQE[I, O]) -> None:
        self._sq.put_nowait(sqe)

    def dequeue(self, n: int) -> list[CQE[O]]:
        cqes: list[CQE[O]] = []
        for _ in range(n):
            try:
                cqe = self._cq.get_nowait()
            except Empty:
                break
            cqes.append(cqe)
        return cqes

    def shutdown(self) -> None:
        self._cq.shutdown()
        self._sq.shutdown()
        for t in self._threads:
            t.join()

        self._threads.clear()
        assert len(self._threads) == 0

        self._sq.join()

    def start(self) -> None:
        for _ in range(self._workers):
            t = Thread(target=self._worker, daemon=True)
            t.start()
            self._threads.append(t)

    def _worker(self) -> None:
        while True:
            try:
                sqe = self._sq.get()
            except ShutDown:
                break
            value: O | Exception
            try:
                value = sqe.value()
            except Exception as e:
                value = e

            self._cq.put_nowait(
                CQE[O](
                    value,
                    sqe.callback,
                )
            )
            self._sq.task_done()
