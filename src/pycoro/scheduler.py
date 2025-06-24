from __future__ import annotations

from collections.abc import Callable, Generator
from queue import Empty, Queue
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pycoro import Computation, Yieldable
    from pycoro.io import IO


class FinalValue[T]:
    def __init__(self, v: T | Exception) -> None:
        self.v = v


class InProcessComputation[T]:
    def __init__(
        self,
        coro: Computation[T],
        next: Any | Exception | Promise | None,
        final: FinalValue[T] | None,
    ) -> None:
        self.coro = coro
        self.next = next
        self.final = final


class Promise: ...


class Scheduler[T]:
    def __init__(self, io: IO[T], size: int) -> None:
        self._io = io
        self._in = Queue[InProcessComputation](size)

        self._running: list[InProcessComputation] = []
        self._awaiting: dict[InProcessComputation, InProcessComputation] = {}

        self._promise_to_computation: dict[Promise, InProcessComputation] = {}

    def add(self, c: Computation[T]) -> None:
        self._in.put_nowait(InProcessComputation(c, None, None))

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

        self.tick()

        assert len(self._running) == 0

    def tick(self) -> None:
        self._unblock()

        while self.step():
            continue

    def step(self) -> bool:
        try:
            computation = self._running.pop()
        except IndexError:
            return False

        assert computation.final is None
        assert isinstance(computation.coro, Generator)

        yielded: Yieldable | FinalValue[T]
        try:
            match computation.next:
                case Exception():
                    yielded = computation.coro.throw(computation.next)
                case _:
                    yielded = computation.coro.send(computation.next)
        except StopIteration as e:
            yielded = FinalValue(e.value)

        match yielded:
            case Promise():
                child_computation = self._promise_to_computation.pop(yielded)

                match child_computation.final:
                    case None:
                        self._awaiting[child_computation] = computation
                    case FinalValue(v=v):
                        computation.next = v
                        self._running.append(computation)

            case FinalValue():
                assert computation.final is None
                computation.final = yielded

            case Generator():
                child_computation = InProcessComputation(yielded, None, None)
                promise = Promise()
                self._promise_to_computation[promise] = child_computation

                self._running.append(child_computation)

                computation.next = promise
                self._running.append(computation)
            case Callable():
                child_computation = InProcessComputation(yielded, None, None)
                promise = Promise()
                self._promise_to_computation[promise] = child_computation

                def _(computation: InProcessComputation[Any], r: Any | Exception) -> None:
                    assert computation.next is None
                    assert computation.final is None
                    computation.final = FinalValue(r)

                self._io.dispatch(yielded, lambda r, comp=child_computation: _(comp, r))

                computation.next = promise
                self._running.append(computation)
        return True

    def _unblock(self) -> None:
        for blocking in list(self._awaiting):
            if blocking.final is None:
                continue
            blocked = self._awaiting.pop(blocking)

            blocked.next = blocking.final.v
            self._running.append(blocked)

    def size(self) -> int:
        return len(self._running) + len(self._awaiting) + self._in.qsize()


def batch[T](queue: Queue[T], n: int, f: Callable[[T], None]) -> None:
    for _ in range(n):
        try:
            e = queue.get_nowait()
        except Empty:
            return
        f(e)
