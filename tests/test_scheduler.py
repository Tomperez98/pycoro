from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, assert_type

from pycoro import Promise, Scheduler
from pycoro.io.function import FunctionIO
from pycoro.typing import typesafe

if TYPE_CHECKING:
    from pycoro import Computation


def test_coroutine_invocation() -> None:
    def coroutine(n: int) -> Computation[Callable[[], str], str]:
        if n == 0:
            return "I finished"

        foo_promise = yield from typesafe(lambda: f"foo.{n}")
        assert_type(foo_promise, Promise)
        bar_promise = yield from typesafe(lambda: f"bar.{n}")
        assert_type(bar_promise, Promise)
        baz_promise = yield from typesafe(coroutine(n - 1))
        assert_type(baz_promise, Promise[str])

        foo = yield from typesafe(foo_promise)
        assert_type(foo, Any)
        bar = yield from typesafe(bar_promise)
        assert_type(bar, Any)
        baz = yield from typesafe(baz_promise)
        assert_type(baz, str)

        return f"{foo}.{bar}.{baz}"

    io = FunctionIO[Callable[[], str], str](100)
    io.worker()

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
    io.shutdown()
