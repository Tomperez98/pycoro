from __future__ import annotations

from collections.abc import Hashable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, assert_never

from pycoro import CQE, SQE

if TYPE_CHECKING:
    from queue import Queue


# Submission
@dataclass(frozen=True)
class StoreSubmission[C: Hashable]:
    transaction: Transaction[C]

    @property
    def kind(self) -> str:
        return "store"


@dataclass(frozen=True)
class Transaction[C: Hashable]:
    cmds: list[C]


# Completion
@dataclass(frozen=True)
class StoreCompletion[R: Hashable]:
    results: list[R]

    @property
    def kind(self) -> str:
        return "store"


class StoreSubsystem[C: Hashable, R: Hashable](Protocol):
    def execute(self, transactions: list[Transaction[C]]) -> list[list[R]]: ...
    def migrate(self) -> None: ...


def process(
    store: StoreSubsystem,
    sqes: list[SQE[StoreSubmission, StoreCompletion]],
) -> list[CQE[StoreCompletion]]:
    transactions: list[Transaction] = []

    for sqe in sqes:
        assert isinstance(sqe.value, StoreSubmission)
        transactions.append(sqe.value.transaction)

    try:
        result = store.execute(transactions)
        assert len(transactions) == len(result), "transactions and results must have equal length"
    except Exception as e:
        result = e

    return [
        CQE(result if isinstance(result, Exception) else StoreCompletion(result[i]), sqe.callback)
        for i, sqe in enumerate(sqes)
    ]


def collect(c: Queue[SQE | int], n: int) -> list[SQE[StoreSubmission, StoreCompletion]]:
    assert n > 0, "batch size must be greater than 0"

    batch: list[SQE] = []
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
