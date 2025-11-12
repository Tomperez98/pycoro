from __future__ import annotations

from uuid import uuid4

import pycoro
from pycoro import aio
from pycoro.app.subsystems.aio import echo
from pycoro.kernel.t_aio import Completion, Submission
from pycoro.kernel.t_aio.echo import EchoCompletion, EchoSubmission


def echo_coroutine(n: int) -> pycoro.CoroutineFunc[Submission, Completion, str]:
    def _(
        c: pycoro.Coroutine[Submission, Completion, str],
    ) -> str:
        if n == 0:
            return ""

        # Yield two I/O operations
        id1 = uuid4().hex
        id2 = uuid4().hex

        foo_future = pycoro.emit(c, Submission({"id": id1}, EchoSubmission(f"foo.{n}")))
        bar_future = pycoro.emit(c, Submission({"id": id2}, EchoSubmission(f"bar.{n}")))
        baz = pycoro.spawn_and_wait(c, echo_coroutine(n - 1))

        # Await results
        foo_completion = pycoro.wait(c, foo_future)
        assert isinstance(foo_completion.value, EchoCompletion)
        assert foo_completion.tags == {"id": id1}
        foo = foo_completion.value.data

        bar_completion = pycoro.wait(c, bar_future)
        assert isinstance(bar_completion.value, EchoCompletion)
        assert bar_completion.tags == {"id": id2}
        bar = bar_completion.value.data

        return f"{foo}:{bar}:{baz}"

    return _


def test_system_aio() -> None:
    # Instantiate IO
    io = aio.new(100)
    io.add_subsystem(echo.new(io, echo.Config()))
    io.start()

    # Instantiate scheduler
    scheduler = pycoro.Scheduler(io, 100)

    # Add coroutine to scheduler
    promise = pycoro.add(scheduler, echo_coroutine(5))
    assert promise

    # Run scheduler until all tasks complete
    i = 0
    while scheduler.size() > 0:
        for cqe in io.dequeue_cqe(3):
            cqe.invoke()
        scheduler.run_until_blocked(i)
        i += 1

    # Shutdown scheduler
    scheduler.shutdown()
    io.shutdown()

    # Await and check final result
    assert promise.result() == "foo.5:bar.5:foo.4:bar.4:foo.3:bar.3:foo.2:bar.2:foo.1:bar.1:"
