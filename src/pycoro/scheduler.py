from __future__ import annotations

from collections import deque
from collections.abc import Generator, Hashable
from concurrent.futures import Future
from queue import Empty, Queue
from typing import TYPE_CHECKING, Any, assert_never, cast

if TYPE_CHECKING:
    from pycoro import io


class Promise[T]: ...


type Yieldable[I, O] = Computation[I, O] | Promise[O] | I
type Computation[I, O] = Generator[Yieldable[I, O], Any, O]


class Handle[O]:
    def __init__(self, f: Future[O]) -> None:
        self._f = f

    def result(self, timeout: float | None = None) -> O:
        return self._f.result(timeout)


class FV[O]:
    def __init__(self, v: O | Exception) -> None:
        self.v = v


class IPC[I, O]:
    def __init__(
        self,
        coro: Computation[I, O] | I,
    ) -> None:
        self.coro = coro

        self.next: O | Exception | Promise[O] | None = None
        self.final: FV[O] | None = None

    def send(self, v: Promise[O] | O | Exception | None) -> Yieldable[I, O] | FV[O]:
        assert isinstance(self.coro, Generator), "can only run send in a computation that's a coroutine"
        try:
            match self.next:
                case Exception():
                    yielded = self.coro.throw(self.next)
                case _:
                    yielded = self.coro.send(self.next)
        except StopIteration as e:
            yielded = FV(e.value)
        except Exception as e:
            yielded = FV(e)

        return yielded


class Scheduler[I: Hashable, O]:
    def __init__(self, io: io.IO[I, O], size: int) -> None:
        self._io = io
        self._in = Queue[tuple[IPC[I, O], Future[O]]](size)

        self._running: deque[IPC[I, O] | tuple[IPC[I, O], Future[O]]] = deque()
        self._awaiting: dict[IPC[I, O], IPC[I, O] | None] = {}

        self._p_to_comp: dict[Promise[O], IPC[I, O]] = {}
        self._comp_to_f: dict[IPC[I, O], Future[O]] = {}

    def add(self, c: Computation[I, O]) -> Handle[O]:
        f = Future[O]()
        self._in.put_nowait((IPC(c), f))
        return Handle(f)

    def shutdown(self) -> None:
        self._in.shutdown()
        self._in.join()
        assert len(self._running) == 0
        assert len(self._awaiting) == 0
        assert len(self._p_to_comp) == 0
        assert len(self._comp_to_f) == 0

    def run_until_blocked(self, time: int) -> None:
        assert len(self._running) == 0

        for _ in range(self._in.qsize()):
            try:
                e = self._in.get_nowait()
            except Empty:
                return
            self._running.appendleft(e)
            self._in.task_done()

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
                case IPC(), Future():
                    comp, future = item
                    assert comp.next is None
                    self._comp_to_f[comp] = future
                case _:
                    assert_never(item)
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
                            case FV(v=v):
                                comp.next = v
                                self._running.appendleft(comp)

                    case FV():
                        self._set(comp, yielded)

                    case Generator():
                        child_comp = IPC[I, O](yielded)
                        promise = Promise()
                        self._p_to_comp[promise] = child_comp

                        self._running.appendleft(child_comp)

                        comp.next = promise
                        self._running.appendleft(comp)

                    case _:
                        child_comp = IPC[I, O](yielded)
                        promise = Promise()
                        self._p_to_comp[promise] = child_comp

                        def _(comp: IPC[I, O], r: O | Exception) -> None:
                            assert comp.next is None
                            self._set(comp, FV(r))

                        self._io.dispatch(cast("I", yielded), lambda r, comp=child_comp: _(comp, r))

                        comp.next = promise
                        self._running.appendleft(comp)

            case _:

                def _(comp: IPC[I, O], r: O | Exception) -> None:
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

    def _set(self, comp: IPC[I, O], final_value: FV[O]) -> None:
        assert comp.final is None
        comp.final = final_value
        if (f := self._comp_to_f.pop(comp, None)) is not None:
            match comp.final.v:
                case Exception():
                    f.set_exception(comp.final.v)
                case _:
                    f.set_result(comp.final.v)
