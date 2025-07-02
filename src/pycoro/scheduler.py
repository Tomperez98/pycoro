from __future__ import annotations

import contextlib
from collections import deque
from collections.abc import Generator, Hashable
from concurrent.futures import Future
from queue import Empty, Queue
from typing import TYPE_CHECKING, Any, cast

from pycoro.bus import SQE

if TYPE_CHECKING:
    from pycoro import io


# commands.
class Promise[T]:
    """Await for computation result."""


class Time:
    """Get current time in milliseconds."""


# types
type _Yieldable[I, O] = Computation[I, O] | Promise[O] | Time | I
type Computation[I, O] = Generator[_Yieldable[I, O], Any, O]


class Handle[O]:
    """Handle for computation result."""

    def __init__(self, f: Future[O]) -> None:
        self._f = f

    def result(self, timeout: float | None = None) -> O:
        """Await for computation result."""
        return self._f.result(timeout)


# internal classes
class _FV[O]:
    def __init__(self, v: O | Exception) -> None:
        self.v = v


class _IPC[I, O]:
    def __init__(
        self,
        coro: Computation[I, O] | I,
    ) -> None:
        self.coro = coro

        self.next: O | Exception | Promise[O] | int | None = None
        self.final: _FV[O] | None = None

        self._pend: list[Promise[O]] = []
        self._final: _FV[O] | None = None

    def send(self, v: Promise[O] | int | O | Exception | None) -> _Yieldable[I, O] | _FV[O]:
        assert isinstance(self.coro, Generator), "can only run send in a computation that's a coroutine"

        if self._final is not None:
            if self._pend:
                return self._pend.pop()
            return self._final

        try:
            match self.next:
                case Exception():
                    yielded = self.coro.throw(self.next)
                case Promise():
                    self._pend.append(self.next)
                    yielded = self.coro.send(self.next)
                case _:
                    yielded = self.coro.send(self.next)
        except StopIteration as e:
            yielded = _FV(e.value)
        except Exception as e:
            yielded = _FV(e)

        match yielded:
            case Promise():
                with contextlib.suppress(ValueError):
                    self._pend.remove(yielded)
                return yielded
            case _FV():
                self._final = yielded
                if self._pend:
                    return self._pend.pop()
                return self._final
            case _:
                return yielded


class Scheduler[I: Hashable, O]:
    def __init__(self, io: io.IO[I, O], size: int) -> None:
        self._io = io
        self._in = Queue[tuple[_IPC[I, O], Future[O]]](size)

        self._running: deque[_IPC[I, O] | tuple[_IPC[I, O], Future[O]]] = deque()
        self._awaiting: dict[_IPC[I, O], _IPC[I, O] | None] = {}

        self._p_to_comp: dict[Promise[O], _IPC[I, O]] = {}
        self._comp_to_f: dict[_IPC[I, O], Future[O]] = {}

    def add(self, c: Computation[I, O] | I) -> Handle[O]:
        """Schedule computation."""
        f = Future[O]()
        self._in.put_nowait((_IPC(c), f))
        return Handle(f)

    def shutdown(self) -> None:
        """Shutdown scheduler."""
        self._io.shutdown()
        self._in.shutdown()
        self._in.join()
        assert len(self._running) == 0
        assert len(self._awaiting) == 0
        assert len(self._p_to_comp) == 0
        assert len(self._comp_to_f) == 0

    def run_until_blocked(self, time: int) -> None:
        """Execute computations until blocked."""
        assert len(self._running) == 0

        qsize = self._in.qsize()
        for _ in range(qsize):
            try:
                e = self._in.get_nowait()
            except Empty:
                return
            self._running.appendleft(e)
            self._in.task_done()

        self.tick(time)

        assert len(self._running) == 0

    def tick(self, time: int) -> None:
        """Tick."""
        self._unblock()

        while self.step(time):
            continue

    def step(self, time: int) -> bool:
        """Take a step."""
        try:
            match item := self._running.pop():
                case _IPC():
                    comp = item
                case _IPC(), Future():
                    comp, future = item
                    assert comp.next is None
                    self._comp_to_f[comp] = future
        except IndexError:
            return False

        assert comp.final is None

        match comp.coro:
            case Generator():
                yielded = comp.send(comp.next)

                match yielded:
                    case Promise():
                        child_comp = self._p_to_comp.pop(yielded)

                        match child_comp.final:
                            case None:
                                self._awaiting[child_comp] = comp
                            case _FV(v=v):
                                comp.next = v
                                self._running.appendleft(comp)

                    case _FV():
                        self._set(comp, yielded)

                    case Generator():
                        child_comp = _IPC[I, O](yielded)
                        promise = Promise()
                        self._p_to_comp[promise] = child_comp
                        self._running.appendleft(child_comp)

                        comp.next = promise
                        self._running.appendleft(comp)

                    case Time():
                        comp.next = time
                        self._running.appendleft(comp)
                    case _:
                        child_comp = _IPC[I, O](yielded)
                        promise = Promise()
                        self._p_to_comp[promise] = child_comp
                        self._io.dispatch(SQE(cast("I", yielded), lambda r, comp=child_comp: self._set(comp, _FV(r))))

                        comp.next = promise
                        self._running.appendleft(comp)

            case _:
                assert comp.next is None
                assert comp not in self._awaiting

                self._io.dispatch(SQE(comp.coro, lambda r, comp=comp: self._set(comp, _FV(r))))
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
        """Total computations on the scheduler."""
        return len(self._running) + len(self._awaiting) + self._in.qsize()

    def _set(self, comp: _IPC[I, O], final_value: _FV[O]) -> None:
        assert comp.final is None
        comp.final = final_value
        if (f := self._comp_to_f.pop(comp, None)) is not None:
            match comp.final.v:
                case Exception():
                    f.set_exception(comp.final.v)
                case _:
                    f.set_result(comp.final.v)
