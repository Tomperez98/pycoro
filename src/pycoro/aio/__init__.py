from __future__ import annotations

from collections.abc import Callable
from queue import Empty, Queue
from typing import Protocol, runtime_checkable

from pycoro.bus import CQE, SQE


@runtime_checkable
class Kind(Protocol):
    @property
    def kind(self) -> str: ...


class SubSystem(Kind, Protocol):
    @property
    def size(self) -> int: ...
    def start(self) -> None: ...
    def shutdown(self) -> None: ...
    def flush(self, time: int) -> None: ...
    def enqueue(self, sqe: SQE) -> bool: ...
    def process(self, sqes: list[SQE]) -> list[CQE]: ...
    def worker(self) -> None: ...


class AIO[I: Kind, O: Kind](Protocol):
    def attach_subsystem(self, subsystem: SubSystem) -> None: ...
    def start(self) -> None: ...
    def shutdown(self) -> None: ...
    def flush(self, time: int) -> None: ...
    def dispatch(self, sqe: SQE[I, O]) -> None: ...
    def dequeue(self, n: int) -> list[CQE[O]]: ...
    def enqueue(self, cqe: tuple[CQE[O], str]) -> None: ...


class AIOSystem[I: Kind, O: Kind]:
    def __init__(self, size: int) -> None:
        self._cq = Queue[tuple[CQE[O], str]](size)
        self._subsystems: dict[str, SubSystem] = {}

    def attach_subsystem(self, subsystem: SubSystem) -> None:
        assert subsystem.size <= self._cq.maxsize, (
            "subsystem size must be equal or less than the AIO size."
        )
        assert subsystem.kind not in self._subsystems, "subsystem is already registered."
        self._subsystems[subsystem.kind] = subsystem

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

    def dispatch(self, sqe: SQE[I, O]) -> None:
        match sqe.value:
            case Callable():
                subsystem = self._subsystems["function"]
            case _:
                subsystem = self._subsystems[sqe.value.kind]

        if not subsystem.enqueue(sqe):
            sqe.callback(NotImplementedError())

    def dequeue(self, n: int) -> list[CQE]:
        cqes: list[CQE] = []
        for _ in range(n):
            try:
                cqe, kind = self._cq.get_nowait()
            except Empty:
                break

            if not isinstance(cqe.value, Exception) and isinstance(cqe.value, Kind):
                assert cqe.value.kind == kind

            cqes.append(cqe)
            self._cq.task_done()
        return cqes

    def enqueue(self, cqe: tuple[CQE, str]) -> None:
        self._cq.put(cqe)
