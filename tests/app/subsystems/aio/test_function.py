from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from pycoro import aio
from pycoro.app.subsystems.aio import function
from pycoro.kernel import bus, t_aio

if TYPE_CHECKING:
    from collections.abc import Callable


def callback_that_asserts(expected: str) -> Callable[[t_aio.Kind | Exception], None]:
    def _(value: t_aio.Kind | Exception) -> None:
        assert not isinstance(value, Exception)
        assert isinstance(value, function.FunctionCompletion)

        assert value.result == expected

    return _


@pytest.mark.parametrize("test_data", ["foo", "bar", "baz"])
def test_echo(test_data: str) -> None:
    subsystem = function.new(aio.new(100), function.Config(workers=1))

    # Equivalent to: assert.Len(t, echo.workers, 1)
    assert len(subsystem.workers) == 1

    # Build SQE with Echo submission data
    sqe = bus.SQE[t_aio.Kind, t_aio.Kind](
        submission=function.FunctionSubmission(fn=lambda: test_data),
        callback=callback_that_asserts(test_data),
    )

    # Process the SQE synchronously through the worker
    cqe = subsystem.process([sqe])[0]
    cqe.invoke()
