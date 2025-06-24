from __future__ import annotations

from collections import deque
from collections.abc import Callable, Generator
from concurrent.futures import Future
from queue import Empty, Queue
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pycoro import Computation, Yieldable
    from pycoro.io import IO


class Handle[T]:
    def __init__(self, f: Future[T]) -> None:
        self._f = f

    def result(self, timeout: float | None = None) -> T:
        return self._f.result(timeout)


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


class Promise[T]: ...


class Scheduler[T]:
    def __init__(self, io: IO[T], size: int) -> None:
        self._io = io
        self._in = Queue[tuple[InProcessComputation, Future]](size)

        self._running: deque[InProcessComputation | tuple[InProcessComputation, Future]] = deque()
        self._awaiting: dict[InProcessComputation, InProcessComputation | None] = {}

        self._p_to_comp: dict[Promise, InProcessComputation] = {}
        self._comp_to_f: dict[InProcessComputation, Future] = {}

    def add(self, c: Computation[T]) -> Handle[T]:
        f = Future[T]()
        self._in.put_nowait((InProcessComputation(c, None, None), f))
        return Handle(f)

    def shutdown(self) -> None:
        self._in.shutdown()
        self._in.join()

    def run_until_blocked(self) -> None:
        assert len(self._running) == 0

        size = self._in.qsize()
        batch(
            self._in,
            size,
            lambda c: self._running.appendleft(c),
        )

        self.tick()

        assert len(self._running) == 0

    def tick(self) -> None:
        self._unblock()

        while self.step():
            continue

    def step(self) -> bool:
        try:
            match item := self._running.pop():
                case InProcessComputation():
                    computation = item
                case _:
                    computation, future = item
                    assert computation.next is None
                    assert computation.final is None

                    self._comp_to_f[computation] = future
        except IndexError:
            return False

        assert computation.final is None
        match computation.coro:
            case Generator():
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
                        child_computation = self._p_to_comp.pop(yielded)

                        match child_computation.final:
                            case None:
                                self._awaiting[child_computation] = computation
                            case FinalValue(v=v):
                                computation.next = v
                                self._running.appendleft(computation)

                    case FinalValue():
                        self._set_final_value(computation, yielded)

                    case Generator():
                        child_computation = InProcessComputation(yielded, None, None)
                        promise = Promise()
                        self._p_to_comp[promise] = child_computation

                        self._running.appendleft(child_computation)

                        computation.next = promise
                        self._running.appendleft(computation)

                    case Callable():
                        child_computation = InProcessComputation(yielded, None, None)
                        promise = Promise()
                        self._p_to_comp[promise] = child_computation

                        def _(computation: InProcessComputation[Any], r: Any | Exception) -> None:
                            assert computation.next is None
                            self._set_final_value(computation, FinalValue(r))

                        self._io.dispatch(yielded, lambda r, comp=child_computation: _(comp, r))

                        computation.next = promise
                        self._running.appendleft(computation)
            case Callable():

                def _(computation: InProcessComputation[Any], r: Any | Exception) -> None:
                    assert computation.next is None
                    self._set_final_value(computation, FinalValue(r))

                self._io.dispatch(computation.coro, lambda r, comp=computation: _(comp, r))
                self._awaiting[computation] = None

        return True

    def _unblock(self) -> None:
        for blocking in list(self._awaiting):
            if blocking.final is None:
                continue
            blocked = self._awaiting.pop(blocking)
            if blocked is not None:
                blocked.next = blocking.final.v
                self._running.appendleft(blocked)

    def size(self) -> int:
        return len(self._running) + len(self._awaiting) + self._in.qsize()

    def _set_final_value(self, computation: InProcessComputation[T], final_value: FinalValue[T]) -> None:
        assert computation.final is None
        computation.final = final_value
        if (f := self._comp_to_f.pop(computation, None)) is not None:
            match computation.final.v:
                case Exception():
                    f.set_exception(computation.final.v)
                case _:
                    f.set_result(computation.final.v)


def batch[T](queue: Queue[T], n: int, f: Callable[[T], None]) -> None:
    for _ in range(n):
        try:
            e = queue.get_nowait()
        except Empty:
            return
        f(e)
        queue.task_done()
