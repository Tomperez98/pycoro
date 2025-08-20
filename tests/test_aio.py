from __future__ import annotations

import random
from typing import TYPE_CHECKING

from pycoro.aio import AIODst, AIOSystem
from pycoro.bus import CQE, SQE
from pycoro.subsystems.echo import EchoCompletion, EchoSubmission, EchoSubsystem
from pycoro.subsystems.function import FunctionSubsystem

if TYPE_CHECKING:
    from collections.abc import Callable


def test_aio_system() -> None:
    def _[T](
        expected: T,
    ) -> Callable[[T | Exception], None]:
        def _(value: T | Exception) -> None:
            assert not isinstance(value, Exception), f"Unexpected exception: {value!r}"
            assert value == expected, f"Value mismatch: got {value!r}, expected {expected!r}"

        return _

    aio = AIOSystem(100)
    function_subsystem = FunctionSubsystem(aio)
    echo_subsystem = EchoSubsystem(aio)
    aio.attach_subsystem(function_subsystem)
    aio.attach_subsystem(echo_subsystem)
    aio.start()

    i = 0
    for a, b in [
        (EchoSubmission("data"), _(EchoCompletion("data"))),
        (lambda: "foo", _("foo")),
    ]:
        aio.dispatch(SQE(a, b))
        i += 1

    cqes: list[CQE] = []
    while len(cqes) < i:
        cqes.extend(aio.dequeue(i))

    for cqe in cqes:
        cqe.cb(cqe.v)

    aio.shutdown()


def test_aio_dst() -> None:
    def _[T](
        expected: T,
    ) -> Callable[[T | Exception], None]:
        def _(value: T | Exception) -> None:
            assert not isinstance(value, Exception), f"Unexpected exception: {value!r}"
            assert value == expected, f"Value mismatch: got {value!r}, expected {expected!r}"

        return _

    aio = AIODst(random.Random(12), 0)
    function_subsystem = FunctionSubsystem(aio)
    echo_subsystem = EchoSubsystem(aio)
    aio.attach_subsystem(function_subsystem)
    aio.attach_subsystem(echo_subsystem)

    i = 0
    for a, b in [
        (EchoSubmission("data"), _(EchoCompletion("data"))),
        (lambda: "foo", _("foo")),
    ]:
        aio.dispatch(SQE(a, b))
        i += 1

    aio.flush(0)

    cqes: list[CQE] = []
    while len(cqes) < i:
        cqes.extend(aio.dequeue(2))

    for cqe in cqes:
        cqe.cb(cqe.v)
