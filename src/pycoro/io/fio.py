from __future__ import annotations

from dataclasses import dataclass
from queue import Empty, Queue, ShutDown
from threading import Thread
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(frozen=True)
class SQE[T]:
    value: Callable[[], T]
    callback: Callable[[T | Exception], None]


@dataclass(frozen=True)
class CQE[T]:
    value: T | Exception
    callback: Callable[[T | Exception], None]


class FIO[T]:
    def __init__(self, size: int) -> None:
        self._sq = Queue[SQE[T]](size)
        self._cq = Queue[CQE[T]](size)
        self._workers: list[Thread] = []

    def dispatch(self, value: Callable[[], T], callback: Callable[[T | Exception], None]) -> None:
        self._sq.put_nowait(SQE(value, callback))

    def enqueue(self, cqe: CQE[T]) -> None:
        self._cq.put_nowait(cqe)

    def dequeue(self, n: int) -> list[CQE[T]]:
        cqes: list[CQE[T]] = []
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
            value: T | Exception
            try:
                value = sqe.value()
            except Exception as e:
                value = e

            self._cq.put_nowait(
                CQE[T](
                    value,
                    sqe.callback,
                )
            )
            self._sq.task_done()

    def worker(self) -> None:
        t = Thread(target=self._worker, daemon=True)
        t.start()
        self._workers.append(t)
