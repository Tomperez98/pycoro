from __future__ import annotations

import queue
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(frozen=True)
class SQE[I, O]:
    value: I
    cb: Callable[[O | Exception], None]


@dataclass(frozen=True)
class CQE[O]:
    result: O | Exception
    cb: Callable[[O | Exception], None]


class FIO[I: Callable[[], Any], O]:
    def __init__(self, size: int) -> None:
        self._sq: Final = queue.Queue[SQE[I, O]](size)
        self._cq: Final = queue.Queue[CQE[O]](size)
        self._threads: list[threading.Thread] = []

    def dispatch(self, v: I | None, cb: Callable[[O | Exception], None]) -> None:
        assert v is not None
        self._sq.put(SQE(v, cb))

    def enqueue(self, cqe: CQE[O]) -> None:
        self._cq.put(cqe)

    def dequeue(self, n: int) -> list[CQE[O]]:
        cqes: list[CQE[O]] = []
        for _ in range(n):
            try:
                cqes.append(self._cq.get_nowait())
            except queue.Empty:
                break
            self._cq.task_done()
        return cqes

    def shutdown(self) -> None:
        self._sq.shutdown()
        self._cq.shutdown()
        self._sq.join()
        self._cq.join()
        for t in self._threads:
            t.join()
        self._threads.clear()

    def worker(self) -> None:
        t = threading.Thread(target=self._worker, daemon=True)
        t.start()
        self._threads.append(t)

    def _worker(self) -> None:
        while True:
            try:
                sqe = self._sq.get()
            except queue.ShutDown:
                break
            result: O | Exception
            try:
                assert sqe.value is not None
                result = sqe.value()
            except Exception as e:
                result = e

            self.enqueue(CQE[O](result, sqe.cb))
            self._sq.task_done()
