from __future__ import annotations

from dataclasses import dataclass
from queue import Full, Queue, ShutDown
from threading import Thread
from typing import TYPE_CHECKING, Final

from pycoro.internal.kernel import t_aio
from pycoro.internal.kernel.bus import CQE, SQE
from pycoro.internal.kernel.t_aio.echo import EchoCompletion, EchoSubmission

if TYPE_CHECKING:
    from pycoro.internal.aio import AIO


@dataclass(frozen=True)
class Config:
    size: int = 100
    batch_size: int = 100
    workers: int = 1


class Echo:
    def __init__(self, aio: AIO, config: Config) -> None:
        self.config: Final = config
        self.aio: Final = aio
        self.sq: Final = Queue[SQE[t_aio.Submission, t_aio.Completion]](config.size)
        self.workers: list[Thread] = [
            Thread(target=self.worker, daemon=True) for _ in range(config.workers)
        ]

    def kind(self) -> str:
        return "echo"

    def start(self) -> None:
        for w in self.workers:
            w.start()

    def stop(self) -> None:
        self.sq.shutdown()
        for w in self.workers:
            w.join()

        self.workers.clear()
        self.sq.join()

    def enqueue(self, sqe: SQE[t_aio.Submission, t_aio.Completion]) -> bool:
        try:
            self.sq.put_nowait(sqe)
        except Full:
            return False
        else:
            return True

    def flush(self, time: int) -> None:  # pyright: ignore[reportUnusedParameter]
        return None

    def process(
        self, sqes: list[SQE[t_aio.Submission, t_aio.Completion]]
    ) -> list[CQE[t_aio.Submission, t_aio.Completion]]:
        assert len(self.workers) > 0, "must be at least one worker"
        cqes: list[CQE[t_aio.Submission, t_aio.Completion]] = []
        for sqe in sqes:
            assert isinstance(sqe.submission.value, EchoSubmission)
            assert self.kind() == sqe.submission.value.kind()
            cqes.append(
                CQE(
                    sqe.id,
                    sqe.callback,
                    t_aio.Completion(
                        sqe.submission.tags, EchoCompletion(sqe.submission.value.data)
                    ),
                )
            )

        return cqes

    def worker(self) -> None:
        while True:
            try:
                sqe = self.sq.get()
            except ShutDown:
                break

            assert sqe.submission.value.kind() == self.kind()
            self.aio.enqueue_cqe(self.process([sqe])[0])
            self.sq.task_done()
