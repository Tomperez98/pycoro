from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pycoro.aio import AIOSystem
from pycoro.pycoro import Pycoro
from pycoro.subsystems.echo import EchoCompletion, EchoSubmission, EchoSubsystem
from pycoro.subsystems.function import FunctionSubsystem

if TYPE_CHECKING:
    from collections.abc import Callable
    from concurrent.futures import Future

    from pycoro.scheduler import Computation


def foo(string: str) -> Computation[EchoSubmission, EchoCompletion]:
    p = yield EchoSubmission(string)
    v = yield p
    assert isinstance(v, EchoCompletion)
    assert v.data == string
    return v


def bar() -> Computation[Callable[[], str], str]:
    p = yield lambda: "foo"
    v = yield p
    assert v == "foo"
    return v


def test_system() -> None:
    aio = AIOSystem(100)
    aio.attach_subsystem(EchoSubsystem(aio))
    aio.attach_subsystem(FunctionSubsystem(aio))
    system = Pycoro(aio, 100, 100)

    system.start()
    _ = system.add(foo("foo")).result()
    _ = system.add(foo("bar")).result()
    _ = system.add(bar()).result()

    futures: list[Future[Any]] = []
    futures.append(system.add(foo("foo")))
    futures.append(system.add(foo("bar")))
    futures.append(system.add(bar()))

    for f in futures:
        f.result()
    system.shutdown()
