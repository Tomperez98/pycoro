from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from queue import Empty, Queue
from typing import Any, Protocol, runtime_checkable

from pycoro.bus import CQE, SQE


@runtime_checkable
class Kind(Protocol):
    @property
    def kind(self) -> str: ...


class SubSystem(Kind, Protocol):
    @property
    def size(self) -> int: ...
    def start(self, cq: Queue[tuple[CQE[Completion], str]]) -> None: ...
    def shutdown(self) -> None: ...
    def flush(self, time: int) -> None: ...
    def enqueue(self, sqe: SQE[Submission, Completion]) -> bool: ...
    def process(self, sqes: list[SQE[Submission, Completion]]) -> list[CQE[Completion]]: ...
    def worker(self, cq: Queue[tuple[CQE[Completion], str]]) -> None: ...


@dataclass(frozen=True)
class Submission[I: Kind]:
    v: I | Callable[[], Any]


@dataclass(frozen=True)
class Completion[O: Kind]:
    v: O | Any


class AIO[I: Kind, O: Kind]:
    def __init__(self, size: int) -> None:
        self._cq = Queue[tuple[CQE[Completion[O]], str]](size)
        self._subsystems: dict[str, SubSystem] = {}

    def attach_subsystem(self, subsystem: SubSystem) -> None:
        assert subsystem.size <= self._cq.maxsize, "subsystem size must be equal or less than the AIO size."
        assert subsystem.kind not in self._subsystems, "subsystem is already registered."
        self._subsystems[subsystem.kind] = subsystem

    def start(self) -> None:
        for subsystem in self._subsystems.values():
            subsystem.start(self._cq)

    def shutdown(self) -> None:
        for subsystem in self._subsystems.values():
            subsystem.shutdown()

        self._cq.shutdown()
        self._cq.join()

    def flush(self, time: int) -> None:
        for subsystem in self._subsystems.values():
            subsystem.flush(time)

    def dispatch(self, sqe: SQE[Submission[I], Completion[O]]) -> None:
        match sqe.value.v:
            case Callable():
                subsystem = self._subsystems["function"]
            case _:
                subsystem = self._subsystems[sqe.value.v.kind]

        if not subsystem.enqueue(sqe):
            sqe.callback(NotImplementedError())

    def dequeue(self, n: int) -> list[CQE[Completion[O]]]:
        cqes: list[CQE[Completion[O]]] = []
        for _ in range(n):
            try:
                cqe, kind = self._cq.get_nowait()
            except Empty:
                break

            if not isinstance(cqe.value, Exception) and isinstance(cqe.value.v, Kind):
                assert cqe.value.v.kind == kind

            cqes.append(cqe)
            self._cq.task_done()
        return cqes
