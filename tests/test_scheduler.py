from __future__ import annotations

from typing import TYPE_CHECKING, assert_type

from pycoro import Promise, Scheduler, typesafe
from pycoro.io.function import FunctionIO

if TYPE_CHECKING:
    from pycoro import Computation


def coroutine(n: int) -> Computation[str]:
    if n == 0:
        return "I finished"

    foo_promise = yield from typesafe(lambda: f"foo.{n}")
    assert_type(foo_promise, Promise[str])
    bar_promise = yield from typesafe(lambda: f"bar.{n}")
    assert_type(bar_promise, Promise[str])
    baz_promise = yield from typesafe(coroutine(n - 1))
    assert_type(baz_promise, Promise[str])

    foo = yield from typesafe(foo_promise)
    assert_type(foo, str)
    bar = yield from typesafe(bar_promise)
    assert_type(bar, str)
    baz = yield from typesafe(baz_promise)
    assert_type(baz, str)
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
