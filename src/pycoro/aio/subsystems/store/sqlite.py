from __future__ import annotations

import contextlib
import sqlite3
from collections.abc import Callable, Hashable
from queue import Full, Queue, ShutDown
from sqlite3 import Connection
from threading import Thread
from typing import TYPE_CHECKING

from pycoro.aio.subsystems.store import (
    StoreCompletion,
    StoreSubmission,
    Transaction,
    collect,
    process,
)
from pycoro.bus import CQE, SQE

if TYPE_CHECKING:
    from pycoro.aio import AIO


class StoreSqliteSubsystem[C: Hashable, R: Hashable]:
    def __init__(
        self,
        aio: AIO[StoreSubmission, StoreCompletion],
        db: str,
        migration_scripts: list[str],
        size: int = 100,
        batch_size: int = 100,
    ) -> None:
        self._aio = aio
        self._sq = Queue[SQE[StoreSubmission, StoreCompletion] | int](size + 1)
        self._cmd_handlers: dict[type[C], Callable[[Connection, C], R]] = {}
        self._thread: Thread | None = None
        self._batch_size = batch_size
        self._db = db
        self._migration_scripts = migration_scripts

    @property
    def size(self) -> int:
        return self._sq.maxsize - 1

    @property
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
        assert self._thread is None
        t = Thread(target=self.worker, daemon=True, name="store-sqlite-worker")
        t.start()
        self._thread = t
        self.migrate()

    def shutdown(self) -> None:
        assert self._thread is not None
        self._sq.shutdown()
        self._thread.join()
        self._thread = None
        self._sq.join()

    def enqueue(self, sqe: SQE[StoreSubmission, StoreCompletion]) -> bool:
        try:
            self._sq.put_nowait(sqe)
        except Full:
            return False
        return True

    def process(self, sqes: list[SQE]) -> list[CQE]:
        return process(self, sqes)

    def flush(self, time: int) -> None:
        with contextlib.suppress(Full):
            self._sq.put_nowait(time)

    def execute(self, transactions: list[Transaction[C]]) -> list[list[R]]:
        conn = sqlite3.connect(self._db, autocommit=False)
        try:
            results: list[list[R]] = []
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

    def add_command_handler(self, cmd: type[C], handler: Callable[[Connection, C], R]) -> None:
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
                assert not isinstance(sqes[0].value, Callable)
                assert sqes[0].value.kind == self.kind
                for cqe in self.process(sqes):
                    self._aio.enqueue((cqe, self.kind))
