from __future__ import annotations

from collections.abc import Callable

from pycoro.io.fio import FIO


def greet(name: str) -> Callable[[], str]:
    def _inner() -> str:
        return f"Hello {name}"

    return _inner


def callback_that_asserts(expected: str) -> Callable[[str | Exception], None]:
    def _callback(value: str | Exception) -> None:
        assert value == expected, f"Greeting value should be `{expected}`, got `{value}`"

    return _callback


def test_fio() -> None:
    fio = FIO[Callable[[], str], str](size=100)

    # Spawn worker thread
    fio.worker()
    fio.worker()
    fio.worker()

    names = ["A", "B", "C", "D"]
    expected_greetings = ["Hello A", "Hello B", "Hello C", "Hello D"]

    # Dispatch tasks
    for name, expected in zip(names, expected_greetings, strict=True):
        fio.dispatch(greet(name), callback_that_asserts(expected))

    # Process completion queue
    n = 0
    while n < len(names):
        cqes = fio.dequeue(1)
        for cqe in cqes:
            cqe.invoke()
            n += 1

    # Shutdown
    fio.shutdown()
