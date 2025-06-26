from __future__ import annotations

from collections.abc import Callable

from pycoro.io.function import FunctionIO


def greet(name: str) -> Callable[[], str]:
    return lambda: f"Hello {name}"


def callback_that_asserts(expected: str) -> Callable[[str | Exception], None]:
    def _(value: str | Exception) -> None:
        assert isinstance(value, str)
        assert value == expected

    return _


def test_fio() -> None:
    fio = FunctionIO[Callable[[], str], str](100)
    fio.worker()
    fio.worker()
    fio.worker()

    names: list[str] = ["A", "B", "C", "D"]
    greetings: list[str] = ["Hello A", "Hello B", "Hello C", "Hello D"]

    for n, g in zip(names, greetings, strict=True):
        fio.dispatch(greet(n), callback_that_asserts(g))

    n = 0
    while n < len(names):
        cqes = fio.dequeue(1)
        if len(cqes) > 0:
            cqe = cqes[0]
            cqe.callback(cqe.value)
            n += 1

    fio.shutdown()
