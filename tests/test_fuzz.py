from __future__ import annotations

import random
from dataclasses import dataclass
from queue import Full
from typing import TYPE_CHECKING

from pycoro import Computation, Promise, Pycoro
from pycoro.aio import AIOSystem, Completion, Submission
from pycoro.aio.subsystems.echo import EchoCompletion, EchoSubmission, EchoSubsystem
from pycoro.aio.subsystems.function import FunctionSubsystem
from pycoro.aio.subsystems.store import StoreCompletion, StoreSubmission, Transaction
from pycoro.aio.subsystems.store.sqlite import StoreSqliteSubsystem
from pycoro.scheduler import Time

if TYPE_CHECKING:
    from concurrent.futures import Future
    from sqlite3 import Connection


type Command = ReadCommand


@dataclass(frozen=True)
class ReadCommand:
    id: int


def read_handler(conn: Connection, cmd: ReadCommand) -> ReadResult:
    conn.execute("INSERT INTO users (value) VALUES (?)", (cmd.id,))
    return ReadResult(cmd.id)


type Result = ReadResult


@dataclass(frozen=True)
class ReadResult:
    id: int


def foo(n: int) -> Computation[Submission[EchoSubmission | StoreSubmission[Command]], Completion[EchoCompletion | StoreCompletion[Result]]]:
    p: Promise | None = None
    for _ in range(n):
        p = yield Submission(StoreSubmission(Transaction([ReadCommand(n) for _ in range(n)])))

    assert p is not None

    v: Completion = yield p

    assert isinstance(v, StoreCompletion)
    assert len(v.results) == n
    return v


def bar(n: int, data: str) -> Computation[Submission[EchoSubmission | StoreSubmission[Command]], Completion[EchoCompletion | StoreCompletion[Result]]]:
    p: Promise | None = None
    for _ in range(n):
        p = yield Submission[EchoSubmission | StoreSubmission[Command]](EchoSubmission(data))
    assert p is not None
    v = yield p
    return Completion(EchoCompletion(v))


def baz(*, recursive: bool = True) -> Computation[Submission, Completion]:
    if not recursive:
        return Completion("I'm done")
    p = yield Submission(lambda: "hi")
    v: Completion = yield p
    assert v.v == "hi"

    now = yield Time()
    assert now >= 0

    yield (yield baz(recursive=False))

    return v


def _run(seed: int) -> None:
    r = random.Random(seed)

    echo_subsystem_size = r.randint(1, 100)
    store_sqlite_subsystem_size = r.randint(1, 100)
    function_subsystem_size = r.randint(1, 100)
    io_size = r.randint(1, 100)

    if store_sqlite_subsystem_size > io_size:
        return

    if echo_subsystem_size > io_size:
        return

    if function_subsystem_size > io_size:
        return

    aio = AIOSystem(io_size)

    echo_subsystem = EchoSubsystem(aio, echo_subsystem_size, r.randint(1, 3))
    store_sqlite_subsystem = StoreSqliteSubsystem(aio, ":memory:", ["CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, value INTEGER)"], store_sqlite_subsystem_size, r.randint(1, 100))
    store_sqlite_subsystem.add_command_handler(ReadCommand, read_handler)
    function_subsystem = FunctionSubsystem(aio, function_subsystem_size, r.randint(1, 3))

    aio.attach_subsystem(echo_subsystem)
    aio.attach_subsystem(store_sqlite_subsystem)
    aio.attach_subsystem(function_subsystem)
    s = Pycoro(aio, r.randint(1, 100), r.randint(1, 100), r.random() * 2)

    n_coros = r.randint(1, 100)
    handles: list[Future[Completion[EchoCompletion | StoreCompletion[Result]]]] = []
    try:
        for _ in range(n_coros):
            match r.randint(0, 3):
                case 0:
                    handles.append(s.add(foo(r.randint(1, 100))))
                case 1:
                    handles.append(s.add(bar(r.randint(1, 100), "hi")))
                case 2:
                    handles.append(s.add(baz()))
                case 3:
                    handles.append(s.add(Submission(lambda: "hi")))
                case _:
                    raise NotImplementedError
    except Full:
        return

    s.shutdown()

    failed: int = 0
    for h in handles:
        try:
            h.result(0)
        except TimeoutError:
            failed += 1
        except Exception:  # noqa: S110
            pass

    assert failed == 0
    return


def test_fuzz() -> None:
    for _ in range(100):
        _run(random.randint(1, 100))
