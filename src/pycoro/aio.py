from __future__ import annotations

from collections.abc import Callable
from queue import Empty, Queue
from typing import TYPE_CHECKING, Any, Protocol

from pycoro.bus import CQE, SQE

if TYPE_CHECKING:
    from random import Random


class Kind(Protocol):
    def kind(self) -> str: ...


class SubSystem(Kind, Protocol):
    def size(self) -> int: ...
    def start(self) -> None: ...
    def shutdown(self) -> None: ...
    def flush(self, time: int) -> None: ...
    def enqueue(self, sqe: SQE) -> bool: ...
    def process(self, sqes: list[SQE]) -> list[CQE]: ...
    def worker(self) -> None: ...


class AIO(Protocol):
    def attach_subsystem(self, subsystem: SubSystem) -> None: ...
    def start(self) -> None: ...
    def shutdown(self) -> None: ...
    def flush(self, time: int) -> None: ...
    def dispatch(self, sqe: SQE) -> None: ...
    def dequeue(self, n: int) -> list[CQE]: ...
    def enqueue(self, cqe: tuple[CQE, str]) -> None: ...


class AIOSystem:
    def __init__(self, size: int) -> None:
        self._cq = Queue[tuple[CQE, str]](size)
        self._subsystems: dict[str, SubSystem] = {}

    def attach_subsystem(self, subsystem: SubSystem) -> None:
        assert subsystem.size() <= self._cq.maxsize, (
            "subsystem size must be equal or less than the AIO size."
        )
        assert subsystem.kind() not in self._subsystems, "subsystem is already registered."
        self._subsystems[subsystem.kind()] = subsystem

    def start(self) -> None:
        for subsystem in self._subsystems.values():
            subsystem.start()

    def shutdown(self) -> None:
        for subsystem in self._subsystems.values():
            subsystem.shutdown()

        self._cq.shutdown()
        self._cq.join()

    def flush(self, time: int) -> None:
        for subsystem in self._subsystems.values():
            subsystem.flush(time)

    def dispatch(self, sqe: SQE) -> None:
        match sqe.v:
            case Callable():
                subsystem = self._subsystems["function"]
            case _:
                subsystem = self._subsystems[sqe.v.kind()]

        if not subsystem.enqueue(sqe):
            sqe.cb(Exception("aio submission queue full"))

    def dequeue(self, n: int) -> list[CQE]:
        cqes: list[CQE] = []
        for _ in range(n):
            try:
                cqe, _ = self._cq.get_nowait()
            except Empty:
                break

            cqes.append(cqe)
            self._cq.task_done()
        return cqes

    def enqueue(self, cqe: tuple[CQE, str]) -> None:
        self._cq.put(cqe)


class AIODst:
    def __init__(self, r: Random, p: float) -> None:
        self._r = r
        self._p = p
        self._subsystems: dict[str, SubSystem] = {}
        self._sqes: list[SQE] = []
        self._cqes: list[CQE] = []

    def attach_subsystem(self, subsystem: SubSystem) -> None:
        assert subsystem.kind() not in self._subsystems, "subsystem is already registered."
        self._subsystems[subsystem.kind()] = subsystem

    def check(self, value: Any) -> Any:
        def _(result: Any | Exception) -> None: ...

        cqe = self._subsystems[value.kind()].process([SQE(value, lambda r: _(r))])[0]
        assert not isinstance(cqe.v, Exception), f"Unexpected exception: {value!r}"
        return cqe.v

    def start(self) -> None:  # pragma: no cover
        msg = "dst shouldn't be spawning workers"
        raise RuntimeError(msg)

    def shutdown(self) -> None:  # pragma: no cover
        msg = "dst shouldn't have spawned workers"
        raise RuntimeError(msg)

    def flush(self, time: int) -> None:
        flush: dict[str, list[SQE]] = {}
        for sqe in self._sqes:
            flush.setdefault(
                sqe.v.kind() if not isinstance(sqe.v, Callable) else "function", []
            ).append(sqe)

        for kind, sqes in flush.items():
            assert kind in self._subsystems, "invalid aio submission"
            to_process: list[SQE] = []
            pre_failure: dict[int, bool] = {}
            post_failure: dict[int, bool] = {}
            n: int = 0

            for i, sqe in enumerate(sqes):
                if self._r.random() < self._p:
                    match self._r.randint(0, 1):
                        case 0:
                            pre_failure[i] = True
                        case 1:
                            post_failure[n] = True

                if pre_failure.get(i, False):
                    self.enqueue(
                        (
                            CQE(Exception("simulated failure before processing"), sqe.cb),
                            "dst",
                        )
                    )
                else:
                    to_process.append(sqe)
                    n += 1

            for i, cqe in enumerate(self._subsystems[kind].process(to_process)):
                if post_failure.get(i, False):
                    self.enqueue(
                        (
                            CQE(Exception("simulated failure after processing"), cqe.cb),
                            "dst",
                        )
                    )
                else:
                    self.enqueue((cqe, "dst"))
        self._sqes.clear()

    def dispatch(self, sqe: SQE) -> None:
        self._sqes.insert(self._r.randrange(len(self._sqes) + 1), sqe)

    def dequeue(self, n: int) -> list[CQE]:
        cqes = self._cqes[: min(n, len(self._cqes))]
        self._cqes = self._cqes[min(n, len(self._cqes)) :]
        return cqes

    def enqueue(self, cqe: tuple[CQE, str]) -> None:
        self._cqes.append(cqe[0])
