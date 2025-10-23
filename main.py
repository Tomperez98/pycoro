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
        print("coroutine:", n)  # noqa: T201

        if n == 0:
            return ""

        # Yield two I/O operations
        foo_future = pycoro.emit(c, lambda: greet(f"foo.{n}"))
        bar_future = pycoro.emit(c, lambda: greet(f"bar.{n}"))

        # Spawn a new coroutine
        baz_future = pycoro.spawn(c, coroutine(n - 1))

        # Await results
        try:
            foo = pycoro.wait(c, foo_future)
        except Exception:
            print("failed fixing.")  # noqa: T201
            foo = f"foo.{n}"

        try:
            bar = pycoro.wait(c, bar_future)
        except Exception:
            print("failed fixing.")  # noqa: T201
            bar = f"bar.{n}"

        try:
            baz = pycoro.wait(c, baz_future)
        except Exception:
            print("failed fixing.")  # noqa: T201
            baz = f"baz.{n}"

        return f"{foo}:{bar}:{baz}"

    return _


def main() -> None:
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
            cqe.cb(cqe.result)
        scheduler.run_until_blocked(0)

    # Shutdown scheduler
    scheduler.shutdown()
    fio.shutdown()

    # Await and print final result
    try:
        value = promise.result()
        print("value:", value)  # noqa: T201
    except Exception as e:
        print("error:", e)  # noqa: T201


if __name__ == "__main__":
    main()
