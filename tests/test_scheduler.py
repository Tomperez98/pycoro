from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, assert_type

import pytest

from pycoro import Scheduler
from pycoro.io.fio import FIO
from pycoro.scheduler import Time

if TYPE_CHECKING:
    from pycoro import Computation


def test_coroutine_invocation() -> None:
    def coroutine(n: int) -> Computation[Callable[[], str], str]:
        if n == 0:
            return "I finished"

        foo_promise = yield lambda: f"foo.{n}"
        bar_promise = yield lambda: f"bar.{n}"
        baz_promise = yield coroutine(n - 1)

        now = yield Time()
        assert now == 0

        foo = yield foo_promise
        bar = yield bar_promise
        baz = yield baz_promise

        return f"{foo}.{bar}.{baz}"

    io = FIO[Callable[[], str], str](100, 1)
    io.start()

    s = Scheduler(io, 100)

    n = 10
    h = s.add(coroutine(n))
    while s.size() > 0:
        cqes = io.dequeue(100)
        for cqe in cqes:
            cqe.callback(cqe.value)

        s.run_until_blocked(0)

    assert h.result().startswith(f"foo.{n}")
    assert h.result().endswith("finished")

    s.shutdown()


def test_coroutine_with_failure() -> None:
    def fail() -> None:
        raise NotImplementedError

    def coroutine(*, exit: bool) -> Computation[Callable[[], None], None]:
        if exit:
            return
        foo_promise = yield fail

        bar_promise = yield coroutine(exit=True)

        foo = yield foo_promise
        assert_type(foo, Any)

        now = yield Time()
        assert now == 1

        _ = yield bar_promise

        now = yield Time()
        assert now == 2

    io = FIO[Callable[[], None], None](100, 1)
    io.start()

    s = Scheduler(io, 100)
    h = s.add(coroutine(exit=False))
    now = 0
    while s.size() > 0:
        cqes = io.dequeue(10)
        for cqe in cqes:
            cqe.callback(cqe.value)

        s.run_until_blocked(now)
        now += 1

    with pytest.raises(NotImplementedError):
        h.result()

    s.shutdown()


def test_function() -> None:
    io = FIO[Callable[[], str], str](100, 1)
    io.start()

    s = Scheduler(io, 100)

    h = s.add(lambda: "hi")
    while s.size() > 0:
        cqes = io.dequeue(100)
        for cqe in cqes:
            cqe.callback(cqe.value)

        s.run_until_blocked(0)

    assert h.result() == "hi"

    s.shutdown()


def test_structure_concurrency() -> None:
    def coro() -> Computation[Callable[[], str], str]:
        _ = yield lambda: "hi"
        _ = yield lambda: "hi"
        _ = yield lambda: "hi"

        return "I'm done"

    io = FIO[Callable[[], str], str](100, 1)
    io.start()

    s = Scheduler(io, 100)

    h = s.add(coro())
    while s.size() > 0:
        cqes = io.dequeue(100)
        for cqe in cqes:
            cqe.callback(cqe.value)

        s.run_until_blocked(0)

    assert h.result() == "I'm done"

    s.shutdown()
