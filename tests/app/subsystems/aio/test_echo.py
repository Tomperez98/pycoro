from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from pycoro import aio
from pycoro.app.subsystems.aio import echo
from pycoro.kernel import bus
from pycoro.kernel.t_aio import Completion, Submission
from pycoro.kernel.t_aio.echo import EchoCompletion, EchoSubmission

if TYPE_CHECKING:
    from collections.abc import Callable


def callback_that_asserts(expected: str) -> Callable[[Completion | Exception], None]:
    def _(value: Completion | Exception) -> None:
        assert not isinstance(value, Exception)
        assert isinstance(value.value, EchoCompletion)
        assert value.value.data == expected

    return _


@pytest.mark.parametrize("test_data", ["foo", "bar", "baz"])
def test_echo(test_data: str) -> None:
    subsystem = echo.Echo(aio.new(100), echo.Config(workers=1))

    # Equivalent to: assert.Len(t, echo.workers, 1)
    assert len(subsystem.workers) == 1

    # Build SQE with Echo submission data
    sqe = bus.SQE[Submission, Completion](
        id="1",
        submission=Submission(tags={"test": "true"}, value=EchoSubmission(data=test_data)),
        callback=callback_that_asserts(test_data),
    )

    # Process the SQE synchronously through the worker
    cqe = subsystem.process([sqe])[0]

    assert cqe.id == sqe.id
    cqe.invoke()
