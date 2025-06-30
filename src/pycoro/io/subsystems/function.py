from __future__ import annotations

from collections.abc import Callable
from queue import Full, Queue, ShutDown
from threading import Thread

from pycoro.bus import CQE, SQE
from pycoro.io.aio import Completion, Submission


class FunctionSubsystem:
    def __init__(self, size: int = 100, workers: int = 1) -> None:
        self._sq = Queue[SQE[Submission, Completion]](size)
        self._workers = workers
        self._threads: list[Thread] = []

    @property
    def size(self) -> int:
        return self._sq.maxsize

    @property
    def kind(self) -> str:
        return "function"

    def start(self, cq: Queue[tuple[CQE[Completion], str]]) -> None:
        assert len(self._threads) == 0

        for i in range(self._workers):
            t = Thread(target=self.worker, args=(cq,), daemon=True, name=f"function-worker-{i}")
            t.start()
            self._threads.append(t)

    def shutdown(self) -> None:
        assert len(self._threads) > 0
        self._sq.shutdown()
        for t in self._threads:
            t.join()

        self._threads.clear()
        assert len(self._threads) == 0, "at least one worker must be set."
        self._sq.join()

    def enqueue(self, sqe: SQE[Submission, Completion]) -> bool:
        try:
            self._sq.put_nowait(sqe)
        except Full:
            return False
        return True

    def flush(self, time: int) -> None:
        return

    def process(self, sqes: list[SQE[Submission, Completion]]) -> list[CQE[Completion]]:
        assert self._workers > 0, "must be at least one worker"
        assert len(sqes) == 1
        sqe = sqes[0]
        assert isinstance(sqe.value.v, Callable)
        return [CQE(Completion(sqe.value.v()), sqe.callback)]

    def worker(self, cq: Queue[tuple[CQE[Completion], str]]) -> None:
        while True:
            try:
                sqe = self._sq.get()
            except ShutDown:
                break

            assert isinstance(sqe.value.v, Callable)

            cq.put((self.process([sqe])[0], "function"))
            self._sq.task_done()
