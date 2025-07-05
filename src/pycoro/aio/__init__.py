from __future__ import annotations

from collections.abc import Callable
from queue import Empty, Queue
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from pycoro.bus import CQE, SQE

if TYPE_CHECKING:
    from random import Random


@runtime_checkable
class Kind(Protocol):
    @property
    def kind(self) -> str: ...


class SubSystem[I: Kind, O: Kind](Kind, Protocol):
    @property
    def size(self) -> int: ...
    def start(self) -> None: ...
    def shutdown(self) -> None: ...
    def flush(self, time: int) -> None: ...
    def enqueue(self, sqe: SQE[I, O]) -> bool: ...
    def process(self, sqes: list[SQE[I, O]]) -> list[CQE[O]]: ...
    def worker(self) -> None: ...


class AIO[I: Kind, O: Kind](Protocol):
    def attach_subsystem(self, subsystem: SubSystem[I, O]) -> None: ...
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
            sqe.callback(Exception("aio submission queue full"))

    def dequeue(self, n: int) -> list[CQE[O]]:
        cqes: list[CQE[O]] = []
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


class AIODst[I: Kind, O: Kind]:
    def __init__(self, r: Random, p: float) -> None:
        self._r = r
        self._p = p
        self._sqes: list[SQE[I, O]] = []
        self._cqes: list[CQE[O]] = []
        self._subsystems: dict[str, SubSystem] = {}

    def attach_subsystem(self, subsystem: SubSystem[I, O]) -> None:
        assert subsystem.kind not in self._subsystems, "subsystem is already registered."
        self._subsystems[subsystem.kind] = subsystem

    def start(self) -> None:
        for subsystem in self._subsystems.values():
            subsystem.start()

    def shutdown(self) -> None:
        for subsystem in self._subsystems.values():
            subsystem.shutdown()

    def flush(self, time: int) -> None:
        flush: dict[str, list[SQE[I, O]]] = {}

        for sqe in self._sqes:
            flush.setdefault(
                sqe.value.kind if isinstance(sqe.value, Kind) else "function", []
            ).append(sqe)

        for kind, sqes in flush.items():
            subsystem = self._subsystems[kind]
            to_process: list[SQE[I, O]] = []

            pre_fail: dict[int, bool] = {}
            post_fail: dict[int, bool] = {}
            n = 0
            for i, sqe in enumerate(sqes):
                if self._r.random() < self._p:
                    if self._r.randint(0, 1) == 0:
                        pre_fail[i] = True
                    else:
                        post_fail[n] = True

                if pre_fail.get(i, False):
                    self._cqes.append(
                        CQE(Exception("simulated failure before processing"), sqe.callback)
                    )
                else:
                    to_process.append(sqe)
                    n += 1

            for i, cqe in enumerate(subsystem.process(to_process)):
                if post_fail.get(i, False):
                    self.enqueue(
                        (
                            CQE(Exception("simulated failure after processing."), cqe.callback),
                            "dst",
                        )
                    )
                else:
                    self.enqueue((cqe, "dst"))

        self._sqes.clear()

    def dispatch(self, sqe: SQE[I, O]) -> None:
        self._sqes.insert(self._r.randint(0, len(self._sqes) + 1), sqe)

    def dequeue(self, n: int) -> list[CQE[O]]:
        result = self._cqes[: min(n, len(self._cqes))]
        self._cqes = self._cqes[min(n, len(self._cqes)) :]
        return result

    def enqueue(self, cqe: tuple[CQE[O], str]) -> None:
        assert cqe[-1] == "dst"
        self._cqes.append(cqe[0])
