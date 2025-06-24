from __future__ import annotations

from typing import TYPE_CHECKING

from pycoro import Scheduler, typesafe
from pycoro.io.function import FunctionIO

if TYPE_CHECKING:
    from pycoro import Computation


def coroutine(n: int) -> Computation[str]:
    if n == 0:
        return "I finished"

    foo_promise = yield from typesafe(lambda: f"foo.{n}")
    bar_promise = yield from typesafe(lambda: f"bar.{n}")
    baz_promise = yield from typesafe(coroutine(n - 1))

    foo = yield from typesafe(foo_promise)
    bar = yield from typesafe(bar_promise)
    baz = yield from typesafe(baz_promise)
    return f"{foo}.{bar}.{baz}"


def test_function_invocation() -> None:
    io = FunctionIO[str](100)
    io.worker()

    s = Scheduler(io, 100)

    h = s.add(lambda: "hi!")
    while s.size() > 0:
        cqes = io.dequeue(100)
        for cqe in cqes:
            cqe.callback(cqe.value)

        s.run_until_blocked(0)

    assert h.result() == "hi!"

    s.shutdown()


def test_coroutine_invocation() -> None:
    io = FunctionIO[str](100)
    io.worker()

    s = Scheduler(io, 100)
    h = s.add(coroutine(3))
    while s.size() > 0:
        cqes = io.dequeue(100)
        for cqe in cqes:
            cqe.callback(cqe.value)

        s.run_until_blocked(0)

    assert h.result().startswith("foo.3")
    assert h.result().endswith("finished")

    s.shutdown()
