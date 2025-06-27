from __future__ import annotations

from collections.abc import Callable
from queue import Empty, Queue, ShutDown
from threading import Thread
from typing import TYPE_CHECKING, Any

from pycoro.bus import CQE, SQE

if TYPE_CHECKING:
    from collections.abc import Callable


class FIO[I: Callable[[], Any], O]:
    def __init__(self, size: int) -> None:
        self._sq = Queue[SQE[I, O]](size)
        self._cq = Queue[CQE[O]](size)
        self._workers: list[Thread] = []

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
        for t in self._workers:
            t.join()

        self._workers.clear()
        assert len(self._workers) == 0

        self._sq.join()

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

    def worker(self) -> None:
        t = Thread(target=self._worker, daemon=True)
        self._workers.append(t)
        t.start()
