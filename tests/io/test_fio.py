from __future__ import annotations

import threading
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
    worker_thread = threading.Thread(target=fio.worker, daemon=True)
    worker_thread.start()

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
            cqe.cb(cqe.result)
            n += 1

    # Shutdown
    fio.shutdown()
    worker_thread.join()
