from __future__ import annotations

import random
from dataclasses import dataclass
from queue import Full
from typing import TYPE_CHECKING

from pycoro import Computation, Promise, Pycoro
from pycoro.io import AIO
from pycoro.io.aio import Completion, Submission
from pycoro.io.subsystems.echo import EchoCompletion, EchoSubmission, EchoSubsystem
from pycoro.io.subsystems.function import FunctionSubsystem
from pycoro.io.subsystems.store import StoreCompletion, StoreSubmission, Transaction
from pycoro.io.subsystems.store.sqlite import StoreSqliteSubsystem
from pycoro.scheduler import Time

if TYPE_CHECKING:
    from sqlite3 import Connection

    from pycoro.scheduler import Handle

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

    echo_subsystem = EchoSubsystem(echo_subsystem_size, r.randint(1, 3))
    store_sqlite_subsystem = StoreSqliteSubsystem(":memory:", ["CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, value INTEGER)"], store_sqlite_subsystem_size, r.randint(1, 100))
    store_sqlite_subsystem.add_command_handler(ReadCommand, read_handler)
    function_subsystem = FunctionSubsystem(function_subsystem_size, r.randint(1, 3))

    io = AIO[EchoSubmission | StoreSubmission[Command], EchoCompletion | StoreCompletion[Result]](io_size)

    io.attach_subsystem(echo_subsystem)
    io.attach_subsystem(store_sqlite_subsystem)
    io.attach_subsystem(function_subsystem)
    s = Pycoro(io, r.randint(1, 100), r.randint(1, 100))

    n_coros = r.randint(1, 100)
    handles: list[Handle[Completion[EchoCompletion | StoreCompletion[Result]]]] = []
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

    s.start()
    s.shutdown()
    assert s.done()

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
