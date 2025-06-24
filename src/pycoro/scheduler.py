from __future__ import annotations

from collections.abc import Callable, Generator
from queue import Empty, Queue
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pycoro import Coroutine
    from pycoro.io import IO


class InProcessCoroutine[T]:
    def __init__(
        self,
        coro: Coroutine[T],
        result: T | Exception | None,
        next_value: Any | Exception | Promise | None,
    ) -> None:
        self.coro = coro
        self.result = result
        self.next_value = next_value


class Promise: ...


class Scheduler[T]:
    def __init__(self, io: IO[T], size: int) -> None:
        self._io = io
        self._in = Queue[InProcessCoroutine](size)

        self._running: list[InProcessCoroutine] = []
        self._awaiting: dict[InProcessCoroutine, InProcessCoroutine] = {}
        self._promise_to_coro: dict[Promise, InProcessCoroutine] = {}

    def add(self, c: Coroutine[T]) -> None:
        self._in.put_nowait(InProcessCoroutine(c, None, None))

    def shutdown(self) -> None:
        self._in.shutdown()
        self._in.join()

    def run_until_blocked(self) -> None:
        assert len(self._running) == 0

        size = self._in.qsize()
        batch(
            self._in,
            size,
            lambda c: self._running.append(c),
        )

        assert len(self._running) == size

        while len(self._running) > 0:
            self.step()

        assert len(self._running) == 0

    def step(self) -> bool:
        try:
            coro = self._running.pop()
        except IndexError:
            return False

        child = _send_next_value(coro)

        match child:
            case Promise():
                raise NotImplementedError

            case Generator():
                self._running.append(InProcessCoroutine(child, None, None))

                coro.next_value = Promise()
                self._running.append(coro)
                self._promise_to_coro[coro.next_value] = coro
                return True

            case Callable():

                def _(r: Any | Exception) -> None:
                    assert coro.result is None
                    coro.result = r

                self._io.dispatch(child, _)

                coro.next_value = Promise()
                self._running.append(coro)
                self._promise_to_coro[coro.next_value] = coro
                return True
            case None:
                return False

    def size(self) -> int:
        return len(self._running) + len(self._awaiting) + self._in.qsize()


def batch[T](queue: Queue[T], n: int, f: Callable[[T], None]) -> None:
    for _ in range(n):
        try:
            e = queue.get_nowait()
        except Empty:
            return
        f(e)


def _send_next_value[T](coro: InProcessCoroutine[T]) -> Callable[[], Any] | Coroutine[Any] | Promise | None:
    assert coro.result is None

    try:
        match coro.next_value:
            case Exception():
                child = coro.coro.throw(coro.next_value)
            case _:
                child = coro.coro.send(coro.next_value)

        coro.next_value = None
    except StopIteration as e:
        raise NotImplementedError from e
    else:
        return child
