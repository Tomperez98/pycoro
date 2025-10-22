from __future__ import annotations

import threading
from typing import Final, Protocol


class Awaitable[T](Protocol):
    def value(self) -> T | Exception: ...
    def pending(self) -> bool: ...
    def completed(self) -> bool: ...


class Promise[T]:
    def __init__(self) -> None:
        self._state: int = 0
        self._value: T | Exception
        self._done: Final = threading.Event()

    def wait(self) -> T | Exception:
        completed = self._done.wait()
        assert completed
        return self._value

    def resolve(self, value: T) -> None:
        if self.completed():
            return
        self._state = 1
        self._value = value
        self._done.set()

    def reject(self, value: Exception) -> None:
        if self.completed():
            return
        self._state = 2
        self._value = value
        self._done.set()

    def complete(self, v: T | Exception) -> None:
        match v:
            case Exception():
                self.reject(v)
            case _:
                self.resolve(v)

    def value(self) -> T | Exception:
        return self._value

    def pending(self) -> bool:
        return self._state == 0

    def completed(self) -> bool:
        return not self.pending()
