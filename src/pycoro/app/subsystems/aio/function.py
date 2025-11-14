from __future__ import annotations

from dataclasses import dataclass
from queue import Full, Queue, ShutDown
from threading import Thread
from typing import TYPE_CHECKING, Any, Final, Literal

from pycoro.kernel.bus import CQE, SQE
from pycoro.kernel.t_aio.function import FunctionCompletion, FunctionSubmission

if TYPE_CHECKING:
    from pycoro.aio import AIO
    from pycoro.kernel.t_api.error import Error


@dataclass(frozen=True)
class Config:
    size: int = 100
    workers: int = 1


def new(aio: AIO, config: Config) -> _Function:
    return _Function(aio, config)


class _Function:
    def __init__(self, aio: AIO, config: Config) -> None:
        self.config: Final = config
        self.aio: Final = aio
        self.sq: Final = Queue[SQE[FunctionSubmission, FunctionCompletion]](config.size)
        self.workers: list[Thread] = [
            Thread(target=self._worker, daemon=True) for _ in range(config.workers)
        ]

    def kind(self) -> Literal["function"]:
        return "function"

    def start(self, errors: Queue[Error] | None) -> None:  # pyright: ignore[reportUnusedParameter]
        for w in self.workers:
            w.start()

    def stop(self) -> None:
        self.sq.shutdown()
        for w in self.workers:
            w.join()

        self.workers.clear()
        self.sq.join()

    def enqueue(self, sqe: SQE[FunctionSubmission, FunctionCompletion]) -> bool:
        try:
            self.sq.put_nowait(sqe)
        except Full:
            return False
        else:
            return True

    def flush(self, time: int) -> None:  # pyright: ignore[reportUnusedParameter]
        return None

    def _process(
        self, sqe: SQE[FunctionSubmission, FunctionCompletion]
    ) -> CQE[FunctionSubmission, FunctionCompletion]:
        assert self.kind() == sqe.submission.kind()

        result: Any | Exception
        try:
            result = sqe.submission.fn()
        except Exception as e:
            result = e
        return CQE(
            sqe.callback,
            FunctionCompletion(result),
        )

    def process(
        self, sqes: list[SQE[FunctionSubmission, FunctionCompletion]]
    ) -> list[CQE[FunctionSubmission, FunctionCompletion]]:
        assert len(self.workers) > 0, "must be at least one worker"
        return [self._process(sqe) for sqe in sqes]

    def _worker(self) -> None:
        while True:
            try:
                sqe = self.sq.get()
            except ShutDown:
                break
            assert sqe.submission.kind() == self.kind()
            self.aio.enqueue_cqe(self._process(sqe))
            self.sq.task_done()
