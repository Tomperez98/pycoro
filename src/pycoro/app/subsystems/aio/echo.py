from __future__ import annotations

from dataclasses import dataclass
from queue import Full, Queue, ShutDown
from threading import Thread
from typing import TYPE_CHECKING, Final, Literal

from pycoro.kernel import t_aio
from pycoro.kernel.bus import CQE, SQE

if TYPE_CHECKING:
    from pycoro.aio import AIO
    from pycoro.kernel.t_api.error import Error


class _Kind:
    def kind(self) -> Literal["echo"]:
        return "echo"


@dataclass(frozen=True)
class EchoSubmission(_Kind):
    data: str


@dataclass(frozen=True)
class EchoCompletion(_Kind):
    data: str


@dataclass(frozen=True)
class Config:
    size: int = 100
    batch_size: int = 100
    workers: int = 1


def new(aio: AIO, config: Config) -> _Echo:
    return _Echo(aio, config)


class _Echo:
    def __init__(self, aio: AIO, config: Config) -> None:
        self.config: Final = config
        self.aio: Final = aio
        self.sq: Final = Queue[SQE[t_aio.Kind, t_aio.Kind]](config.size)
        self.workers: list[Thread] = [
            Thread(target=self._worker, daemon=True) for _ in range(config.workers)
        ]

    def kind(self) -> Literal["echo"]:
        return "echo"

    def start(self, errors: Queue[Error] | None) -> None:  # pyright: ignore[reportUnusedParameter]
        for w in self.workers:
            w.start()

    def stop(self) -> None:
        self.sq.shutdown()
        for w in self.workers:
            w.join()

        self.workers.clear()
        self.sq.join()

    def enqueue(self, sqe: SQE[t_aio.Kind, t_aio.Kind]) -> bool:
        try:
            self.sq.put_nowait(sqe)
        except Full:
            return False
        else:
            return True

    def flush(self, time: int) -> None:  # pyright: ignore[reportUnusedParameter]
        return None

    def _process(self, sqe: SQE[t_aio.Kind, t_aio.Kind]) -> CQE[t_aio.Kind, t_aio.Kind]:
        assert self.kind() == sqe.submission.kind()
        assert isinstance(sqe.submission, EchoSubmission)

        return CQE(
            sqe.callback,
            EchoCompletion(sqe.submission.data),
        )

    def process(self, sqes: list[SQE[t_aio.Kind, t_aio.Kind]]) -> list[CQE[t_aio.Kind, t_aio.Kind]]:
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
