from __future__ import annotations

import contextlib
from collections.abc import Hashable
from queue import Full, Queue, ShutDown
from threading import Thread

from pycoro.bus import CQE, SQE
from pycoro.io.aio import Completion, Submission
from pycoro.io.subsystems.store import StoreCompletion, StoreSubmission, Transaction, collect, process


class StoreSqliteSubsystem[C: Hashable, R: Hashable]:
    def __init__(self, size: int = 100, batch_size: int = 100) -> None:
        self._sq = Queue[SQE[Submission[StoreSubmission[C]], Completion[StoreCompletion[R]]] | int](size + 1)
        self._thread: Thread | None = None
        self._batch_size = batch_size

    @property
    def size(self) -> int:
        return self._sq.maxsize - 1

    @property
    def kind(self) -> str:
        return "store"

    def start(self, cq: Queue[tuple[CQE[Completion[StoreCompletion[R]]], str]]) -> None:
        assert self._sq.qsize() == 0
        assert self._thread is None
        t = Thread(target=self.worker, args=(cq,), daemon=True, name="store-sqlite-worker")
        t.start()
        self._thread = t

    def shutdown(self) -> None:
        assert self._thread is not None
        self._sq.shutdown()
        self._thread.join()
        self._thread = None
        self._sq.join()

    def enqueue(self, sqe: SQE[Submission[StoreSubmission[C]], Completion[StoreCompletion[R]]]) -> bool:
        try:
            self._sq.put_nowait(sqe)
        except Full:
            return False
        return True

    def process(self, sqes: list[SQE[Submission[StoreSubmission[C]], Completion[StoreCompletion[R]]]]) -> list[CQE[Completion[StoreCompletion[R]]]]:
        return process(self, sqes)

    def flush(self, time: int) -> None:
        with contextlib.suppress(Full):
            self._sq.put_nowait(time)

    def execute(self, transactions: list[Transaction[C]]) -> list[list[R]]:
        results: list[list[R]] = []
        for transaction in transactions:
            assert len(transaction.cmds) > 0, "expect a command"

            cmds_results: list[R] = []
            for cmd in transaction.cmds:
                raise NotImplementedError
            results.append(cmds_results)
        return results

    def worker(self, cq: Queue[tuple[CQE[Completion[StoreCompletion[R]]], str]]) -> None:
        while True:
            try:
                sqes = collect(self._sq, self._batch_size)
            except ShutDown:
                break

            assert len(sqes) <= self._batch_size
            if len(sqes) > 0:
                assert sqes[0].value.v.kind == self.kind
                for cqe in self.process(sqes):
                    cq.put((cqe, sqes[0].value.v.kind))
