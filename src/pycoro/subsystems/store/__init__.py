from __future__ import annotations

from collections.abc import Hashable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, assert_never

from pycoro.bus import CQE, SQE

if TYPE_CHECKING:
    from queue import Queue

KIND = "store"


# Submission
@dataclass(frozen=True)
class StoreSubmission:
    transaction: Transaction

    def kind(self) -> str:
        return KIND


@dataclass(frozen=True)
class Transaction[T: Hashable]:
    cmds: list[T]


# Completion
@dataclass(frozen=True)
class StoreCompletion:
    results: list[Any]

    def kind(self) -> str:
        return KIND


class StoreSubsystem(Protocol):
    def execute(self, transactions: list[Transaction]) -> list[list[Any]]: ...


def process(
    store: StoreSubsystem,
    sqes: list[SQE[StoreSubmission, StoreCompletion]],
) -> list[CQE[StoreCompletion]]:
    transactions = [sqe.v.transaction for sqe in sqes]

    try:
        result = store.execute(transactions)
        assert len(transactions) == len(result), "transactions and results must have equal length"
    except Exception as e:
        result = e

    return [
        CQE(result if isinstance(result, Exception) else StoreCompletion(result[i]), sqe.cb)
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
            case _:
                assert_never(sqe)

    return batch
