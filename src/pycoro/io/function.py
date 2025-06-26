from __future__ import annotations

from dataclasses import dataclass
from queue import Empty, Queue, ShutDown
from threading import Thread
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(frozen=True)
class SQE[I: Callable[[], Any], O]:
    value: I
    callback: Callable[[O | Exception], None]


@dataclass(frozen=True)
class CQE[O]:
    value: O | Exception
    callback: Callable[[O | Exception], None]


class FunctionIO[I: Callable[[], Any], O]:
    def __init__(self, size: int) -> None:
        self._sq = Queue[SQE[I, O]](size)
        self._cq = Queue[CQE[O]](size)
        self._workers: list[Thread] = []

    def dispatch(self, value: I, callback: Callable[[O | Exception], None]) -> None:
        self._sq.put_nowait(SQE(value, callback))

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
        t.start()
        self._workers.append(t)
