from __future__ import annotations

from queue import Empty, Queue
from typing import TYPE_CHECKING, Protocol

from pycoro.kernel import bus, t_aio
from pycoro.kernel.t_aio.store import StoreCompletion, StoreSubmission

if TYPE_CHECKING:
    from pycoro.kernel.t_aio.store import Transaction


class Store(Protocol):
    def execute(self, transactions: list[Transaction]) -> list[StoreCompletion]: ...


def process(
    store: Store, sqes: list[bus.SQE[t_aio.Submission, t_aio.Completion]]
) -> list[bus.CQE[t_aio.Submission, t_aio.Completion]]:
    cqes: list[bus.CQE[t_aio.Submission, t_aio.Completion]] = []
    transactions: list[Transaction] = []

    for sqe in sqes:
        assert isinstance(sqe.submission.value, StoreSubmission)
        transactions.append(sqe.submission.value.transaction)

    err: Exception | None = None
    results: list[StoreCompletion] | None = None
    try:
        results = store.execute(transactions)
    except Exception as e:
        err = e
    else:
        assert len(transactions) == len(results), "transactions and results must have equal length"

    for i, sqe in enumerate(sqes):
        cqe: bus.CQE[t_aio.Submission, t_aio.Completion]
        if err is not None:
            cqe = bus.CQE(id=sqe.id, callback=sqe.callback, completion=err)
        else:
            assert results is not None
            cqe = bus.CQE(
                id=sqe.id,
                callback=sqe.callback,
                completion=t_aio.Completion(
                    tags=sqe.submission.tags,
                    value=results[i],
                ),
            )
        cqes.append(cqe)

    return cqes


def collect(
    c: Queue[bus.SQE[t_aio.Submission, t_aio.Completion] | int], n: int
) -> tuple[list[bus.SQE[t_aio.Submission, t_aio.Completion]], bool]:
    assert n > 0, "batch size must be greater than 0"

    batch: list[bus.SQE[t_aio.Submission, t_aio.Completion]] = []

    for _ in range(n):
        try:
            item = c.get_nowait()
        except Empty:
            return batch, False

        if isinstance(item, int):
            return batch, True

        batch.append(item)

    return batch, True
