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


class FV[T]:
    def __init__(self, v: T | Exception) -> None:
        self.v = v


class IPC[T]:
    def __init__(
        self,
        coro: Computation[T],
        next: Any | Exception | P | None,
        final: FV[T] | None,
    ) -> None:
        self.coro = coro
        self.next = next
        self.final = final


class P[T]: ...


class Scheduler[T]:
    def __init__(self, io: IO[T], size: int) -> None:
        self._io = io
        self._in = Queue[tuple[IPC[T], Future[T]]](size)

        self._running: deque[IPC[T] | tuple[IPC[T], Future[T]]] = deque()
        self._awaiting: dict[IPC[T], IPC[T] | None] = {}

        self._p_to_comp: dict[P[T], IPC[T]] = {}
        self._comp_to_f: dict[IPC[T], Future[T]] = {}

    def add(self, c: Computation[T]) -> Handle[T]:
        f = Future[T]()
        self._in.put_nowait((IPC(c, None, None), f))
        return Handle(f)

    def shutdown(self) -> None:
        self._in.shutdown()
        self._in.join()
        assert len(self._running) == 0
        assert len(self._awaiting) == 0
        assert len(self._p_to_comp) == 0
        assert len(self._comp_to_f) == 0
        self._io.shutdown()

    def run_until_blocked(self, time: int) -> None:
        assert len(self._running) == 0

        size = self._in.qsize()
        batch(
            self._in,
            size,
            lambda c: self._running.appendleft(c),
        )

        self.tick(time)

        assert len(self._running) == 0

    def tick(self, time: int) -> None:
        self._unblock()

        while self.step(time):
            continue

    def step(self, time: int) -> bool:
        try:
            match item := self._running.pop():
                case IPC():
                    comp = item
                case _:
                    comp, future = item
                    assert comp.next is None
                    assert comp.final is None

                    self._comp_to_f[comp] = future
        except IndexError:
            return False

        assert comp.final is None
        match comp.coro:
            case Generator():
                yielded: Yieldable | FV[T]
                try:
                    match comp.next:
                        case Exception():
                            yielded = comp.coro.throw(comp.next)
                        case _:
                            yielded = comp.coro.send(comp.next)
                except StopIteration as e:
                    yielded = FV(e.value)

                match yielded:
                    case P():
                        child_comp = self._p_to_comp.pop(yielded)

                        match child_comp.final:
                            case None:
                                self._awaiting[child_comp] = comp
                            case FV(v=v):
                                comp.next = v
                                self._running.appendleft(comp)

                    case FV():
                        self._set(comp, yielded)

                    case Generator():
                        child_comp = IPC(yielded, None, None)
                        promise = P()
                        self._p_to_comp[promise] = child_comp

                        self._running.appendleft(child_comp)

                        comp.next = promise
                        self._running.appendleft(comp)

                    case Callable():
                        child_comp = IPC(yielded, None, None)
                        promise = P()
                        self._p_to_comp[promise] = child_comp

                        def _(comp: IPC[Any], r: Any | Exception) -> None:
                            assert comp.next is None
                            self._set(comp, FV(r))

                        self._io.dispatch(yielded, lambda r, comp=child_comp: _(comp, r))

                        comp.next = promise
                        self._running.appendleft(comp)
            case Callable():

                def _(comp: IPC[Any], r: Any | Exception) -> None:
                    assert comp.next is None
                    self._set(comp, FV(r))

                self._io.dispatch(comp.coro, lambda r, comp=comp: _(comp, r))
                self._awaiting[comp] = None

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

    def _set(self, comp: IPC[T], final_value: FV[T]) -> None:
        assert comp.final is None
        comp.final = final_value
        if (f := self._comp_to_f.pop(comp, None)) is not None:
            match comp.final.v:
                case Exception():
                    f.set_exception(comp.final.v)
                case _:
                    f.set_result(comp.final.v)


def batch[T](queue: Queue[T], n: int, f: Callable[[T], None]) -> None:
    for _ in range(n):
        try:
            e = queue.get_nowait()
        except Empty:
            return
        f(e)
        queue.task_done()
