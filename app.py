from __future__ import annotations

from typing import TYPE_CHECKING

from pycoro.io.fio import FIO
from pycoro.scheduler import Scheduler

if TYPE_CHECKING:
    from pycoro import Computation


def coroutine(n: int) -> Computation[str]:
    if n == 0:
        return "I finished"

    foo_promise = yield (lambda: f"foo.{n}")
    bar_promise = yield (lambda: f"bar.{n}")
    baz_promise = yield (coroutine(n - 1))

    foo = yield foo_promise
    bar = yield bar_promise
    baz = yield baz_promise
    return f"{foo}.{bar}.{baz}"


def main() -> None:
    io = FIO[str](100)
    io.worker()

    s = Scheduler(io, 100)
    h = s.add(coroutine(50))
    while s.size() > 0:
        cqes = io.dequeue(100)
        for cqe in cqes:
            cqe.callback(cqe.value)

        s.run_until_blocked()

    h.result()


main()
