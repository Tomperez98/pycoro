from __future__ import annotations

import contextlib
import sqlite3
from collections.abc import Hashable
from queue import Full, Queue, ShutDown
from sqlite3 import Connection
from threading import Thread
from typing import TYPE_CHECKING, Any

from pycoro.bus import CQE, SQE
from pycoro.subsystems.store import (
    StoreCompletion,
    StoreSubmission,
    Transaction,
    collect,
    process,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from pycoro.aio import AIO


class StoreSqliteSubsystem:
    def __init__(
        self,
        aio: AIO,
        db: str,
        migration_scripts: list[str],
        size: int = 100,
        batch_size: int = 100,
    ) -> None:
        self._aio = aio
        self._sq = Queue[SQE[StoreSubmission, StoreCompletion] | int](size + 1)
        self._cmd_handlers: dict[type[Hashable], Callable[[Connection, Any], Any]] = {}
        self._thread: Thread | None = None
        self._batch_size = batch_size
        self._db = db
        self._migration_scripts = migration_scripts

    def size(self) -> int:
        return self._sq.maxsize - 1

    def kind(self) -> str:
        return "store"

    def migrate(self) -> None:
        conn = sqlite3.connect(self._db)
        try:
            for script in self._migration_scripts:
                conn.execute(script)
            conn.commit()
        except Exception:
            conn.rollback()
            conn.close()
            raise

        conn.close()

    def start(self) -> None:
        assert self._thread is None, "Thread already started"
        t = Thread(target=self.worker, daemon=True)
        t.start()
        self._thread = t

    def shutdown(self) -> None:
        assert self._thread is not None, "No thread running to shut down"
        self._sq.shutdown()
        self._thread.join()
        self._thread = None
        self._sq.join()

    def enqueue(self, sqe: SQE[StoreSubmission, StoreCompletion]) -> bool:
        assert isinstance(sqe.v, StoreSubmission), (
            f"Expected StoreSubmission, got {type(sqe.v).__name__}"
        )
        assert sqe.v.kind() == self.kind(), (
            f"Kind mismatch: sqe.v.kind()={sqe.v.kind()} != self.kind()={self.kind()}"
        )

        try:
            self._sq.put_nowait(sqe)
        except Full:
            return False
        return True

    def process(
        self, sqes: list[SQE[StoreSubmission, StoreCompletion]]
    ) -> list[CQE[StoreCompletion]]:
        return process(self, sqes)

    def flush(self, time: int) -> None:
        with contextlib.suppress(Full):
            self._sq.put_nowait(time)

    def execute(self, transactions: list[Transaction]) -> list[list[Any]]:
        conn = sqlite3.connect(self._db, autocommit=False)
        try:
            results: list[list[Any]] = []
            for transaction in transactions:
                assert len(transaction.cmds) > 0, "expect a command"
                results.append(
                    [self._cmd_handlers[type(cmd)](conn, cmd) for cmd in transaction.cmds],
                )

            conn.commit()
        except Exception:
            conn.rollback()
            conn.close()
            raise

        conn.close()
        return results

    def add_command_handler[T: Hashable](
        self, cmd: type[T], handler: Callable[[Connection, T], Any]
    ) -> None:
        assert cmd not in self._cmd_handlers
        self._cmd_handlers[cmd] = handler

    def worker(self) -> None:
        while True:
            try:
                sqes = collect(self._sq, self._batch_size)
            except ShutDown:
                break

            assert len(sqes) <= self._batch_size

            if len(sqes) > 0:
                for cqe in self.process(sqes):
                    self._aio.enqueue((cqe, self.kind()))
