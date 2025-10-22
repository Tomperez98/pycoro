from __future__ import annotations

import queue
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Protocol

if TYPE_CHECKING:
    from collections.abc import Callable

    from pycoro.io import io


class Coroutine[I, O](Protocol):
    def resume(
        self,
    ) -> tuple[
        I | None,
        Promise[O] | None,
        Coroutine[I, O] | None,
        Completable | None,
        bool,
    ]: ...
    def set_time(self, time: int) -> None: ...


class Promise[T](Protocol):
    def complete(self, v: T | Exception) -> None: ...


class Completable(Protocol):
    def pending(self) -> bool: ...
    def completed(self) -> bool: ...


@dataclass(frozen=True)
class AwaitingCoroutine[I, O]:
    coroutine: Coroutine[I, O]
    on: Completable


class Scheduler[I, O]:
    def __init__(self, io: io.IO[I, O], size: int) -> None:
        self._io: Final = io
        self._in: Final = queue.Queue[Coroutine[I, O]](size)
        self._runnable: list[Coroutine[I, O]] = []
        self._awaiting: list[AwaitingCoroutine[I, O]] = []
        self._closed: bool = False

    def add(self, c: Coroutine[I, O]) -> bool:
        if self._closed:
            return False

        try:
            self._in.put_nowait(c)
        except queue.Full:
            return False
        return True

    def run_until_blocked(self, time: int) -> None:
        batch(self._in, self._in.qsize(), lambda c: self._runnable.append(c))
        self.tick(time)
        assert len(self._runnable) == 0, "runnable should be empty"

    def tick(self, time: int) -> None:
        self.unblock()

        while True:
            ok = self.step(time)
            if not ok:
                break

    def step(self, time: int) -> bool:
        coroutine = dequeue(self._runnable)
        if coroutine is None:
            return False
        coroutine.set_time(time)

        value, promise, spawn, wait, done = coroutine.resume()
        if promise is not None:
            self._io.dispatch(value, promise.complete)
            self._runnable.append(coroutine)
        elif spawn is not None:
            self._runnable.append(spawn)
            self._runnable.append(coroutine)
        elif wait is not None:
            self._awaiting.append(AwaitingCoroutine[I, O](coroutine, wait))
        elif done:
            self.unblock()
        else:
            msg = "unreachable"
            raise AssertionError(msg)
        return True

    def size(self) -> int:
        return len(self._runnable) + len(self._awaiting) + self._in.qsize()

    def shutdown(self) -> None:
        self._closed = True
        self._in.shutdown()
        self._in.join()

    def unblock(self) -> None:
        i = 0
        for coroutine in self._awaiting:
            if coroutine.on.completed():
                self._runnable.append(coroutine.coroutine)
            else:
                self._awaiting[i] = coroutine
                i += 1

        self._awaiting = self._awaiting[:i]


def batch[T](c: queue.Queue[T], n: int, f: Callable[[T], None]) -> None:
    for _ in range(n):
        try:
            item = c.get_nowait()
        except queue.Empty:
            return

        f(item)
        c.task_done()


def dequeue[T](c: list[T]) -> T | None:
    try:
        item = c.pop(0)
    except IndexError:
        return None
    return item
