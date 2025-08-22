from __future__ import annotations

from collections.abc import Hashable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, assert_never

from pycoro.bus import CQE, SQE

if TYPE_CHECKING:
    from queue import Queue


# Submission
@dataclass(frozen=True)
class StoreSubmission:
    transaction: Transaction

    def kind(self) -> str:
        return "store"


@dataclass(frozen=True)
class Transaction[T: Hashable]:
    cmds: list[T]


# Completion
@dataclass(frozen=True)
class StoreCompletion:
    results: list[Any]

    def kind(self) -> str:
        return "store"


class StoreSubsystem(Protocol):
    def execute(self, transactions: list[Transaction]) -> list[list[Any]]: ...


def process(
    store: StoreSubsystem,
    sqes: list[SQE[StoreSubmission, StoreCompletion]],
) -> list[CQE[StoreCompletion]]:
    transactions = [sqe.v.transaction for sqe in sqes if isinstance(sqe.v, StoreSubmission)]
    assert len(transactions) == len(sqes), "All SQEs must wrap a StoreSubmission"

    try:
        results = store.execute(transactions)
        assert len(results) == len(transactions), "Transactions and results must have equal length"
    except Exception as e:
        results = e

    return [
        CQE(results if isinstance(results, Exception) else StoreCompletion(results[i]), sqe.cb)
        for i, sqe in enumerate(sqes)
    ]


def collect(
    c: Queue[SQE[StoreSubmission, StoreCompletion] | int], n: int
) -> list[SQE[StoreSubmission, StoreCompletion]]:
    assert n > 0, "batch size must be greater than 0"

    batch: list[SQE[StoreSubmission, StoreCompletion]] = []
    for _ in range(n):
        sqe = c.get()

        match sqe:
            case SQE():
                batch.append(sqe)
                c.task_done()
            case int():
                c.task_done()
                return batch
            case _:  # pragma: no cover
                assert_never(sqe)

    return batch
