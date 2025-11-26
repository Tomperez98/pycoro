from __future__ import annotations

from typing import TYPE_CHECKING

import pycoro
from pycoro import aio
from pycoro.app.subsystems.aio import echo, function

if TYPE_CHECKING:
    from pycoro.kernel import t_aio


def function_coroutine(
    n: int,
) -> pycoro.CoroutineFunc[t_aio.Kind, t_aio.Kind, str]:
    def _(
        c: pycoro.Coroutine[t_aio.Kind, t_aio.Kind, str],
    ) -> str:
        if n == 0:
            return ""

        foo_future = pycoro.emit(c, function.FunctionSubmission(lambda: f"foo.{n}"))
        bar_future = pycoro.emit(c, function.FunctionSubmission(lambda: f"bar.{n}"))
        baz = pycoro.spawn_and_wait(c, function_coroutine(n - 1))

        # Await results
        foo_completion = pycoro.wait(c, foo_future)
        assert isinstance(foo_completion, function.FunctionCompletion)
        foo = foo_completion.result
        assert isinstance(foo, str)

        bar_completion = pycoro.wait(c, bar_future)
        assert isinstance(bar_completion, function.FunctionCompletion)
        bar = bar_completion.result
        assert isinstance(bar, str)

        return f"{foo}:{bar}:{baz}"

    return _


def echo_coroutine(n: int) -> pycoro.CoroutineFunc[t_aio.Kind, t_aio.Kind, str]:
    def _(
        c: pycoro.Coroutine[t_aio.Kind, t_aio.Kind, str],
    ) -> str:
        if n == 0:
            return ""

        # Yield two I/O operations
        foo_future = pycoro.emit(c, echo.EchoSubmission(f"foo.{n}"))
        bar_future = pycoro.emit(c, echo.EchoSubmission(f"bar.{n}"))
        baz = pycoro.spawn_and_wait(c, echo_coroutine(n - 1))

        # Await results
        foo_completion = pycoro.wait(c, foo_future)
        assert isinstance(foo_completion, echo.EchoCompletion)
        foo = foo_completion.data

        bar_completion = pycoro.wait(c, bar_future)
        assert isinstance(bar_completion, echo.EchoCompletion)
        bar = bar_completion.data

        return f"{foo}:{bar}:{baz}"

    return _


def test_system() -> None:
    # Instantiate IO
    io = aio.new(100)
    io.add_subsystem(echo.new(io, echo.Config()))
    io.add_subsystem(function.new(io, function.Config()))
    io.start()

    # Instantiate scheduler
    scheduler = pycoro.Scheduler(io, 100)

    # Add coroutine to scheduler
    echo_promise = pycoro.add(scheduler, echo_coroutine(5))
    function_promise = pycoro.add(scheduler, function_coroutine(5))
    assert echo_promise is not None
    assert function_promise is not None

    # Run scheduler until all tasks complete
    i = 0
    while scheduler.size() > 0:
        for cqe in io.dequeue_cqe(3):
            cqe.invoke()
        scheduler.run_until_blocked(i)
        i += 1

    # Shutdown scheduler
    io.shutdown()
    scheduler.shutdown()

    # Await and check final result
    assert echo_promise.result() == function_promise.result()
