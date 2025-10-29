from __future__ import annotations

import random
from collections.abc import Callable

import pycoro
from pycoro.io.fio import FIO


def greet(string: str) -> str:
    if random.randint(0, 1) == 0:
        msg = "oops"
        raise RuntimeError(msg)

    return string


def coroutine(n: int) -> pycoro.CoroutineFunc[Callable[[], str], str, str]:
    def _(
        c: pycoro.Coroutine[Callable[[], str], str, str],
    ) -> str:
        if n == 0:
            return ""

        # Yield two I/O operations
        foo_future = pycoro.emit(c, lambda: greet(f"foo.{n}"))
        bar_future = pycoro.emit(c, lambda: greet(f"bar.{n}"))
        try:
            baz = pycoro.spawn_and_wait(c, coroutine(n - 1))
        except Exception:
            baz = f"baz.{n}"

        # Await results
        try:
            foo = pycoro.wait(c, foo_future)
        except Exception:
            foo = f"foo.{n}"

        try:
            bar = pycoro.wait(c, bar_future)
        except Exception:
            bar = f"bar.{n}"

        return f"{foo}:{bar}:{baz}"

    return _


def test_system() -> None:
    # Instantiate FIO
    fio = FIO[Callable[[], str], str](100)

    # Start I/O worker on a thread
    fio.worker()

    # Instantiate scheduler
    scheduler = pycoro.Scheduler[Callable[[], str], str](fio, 100)

    # Add coroutine to scheduler
    promise = pycoro.add(scheduler, coroutine(3))
    assert promise

    # Run scheduler until all tasks complete
    while scheduler.size() > 0:
        for cqe in fio.dequeue(3):
            cqe.invoke()
        scheduler.run_until_blocked(0)

    # Shutdown scheduler
    scheduler.shutdown()
    fio.shutdown()

    # Await and check final result
    assert promise.result() == "foo.3:bar.3:foo.2:bar.2:foo.1:bar.1:"
