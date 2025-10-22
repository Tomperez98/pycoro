from __future__ import annotations

import queue
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from random import Random

type Callback[O] = Callable[[O | Exception], None]


@dataclass(frozen=True)
class SQE[I: Kind | Callable[[], Any], O]:
    v: I
    cb: Callback[O]


@dataclass(frozen=True)
class CQE[O]:
    v: O | Exception
    cb: Callback[O]


class Kind(Protocol):
    def kind(self) -> str: ...


class Subsystem(Kind, Protocol):
    def size(self) -> int: ...
    def start(self) -> None: ...
    def shutdown(self) -> None: ...
    def flush(self, time: int) -> None: ...
    def enqueue(self, sqe: SQE[Any, Any]) -> bool: ...
    def process(self, sqes: list[SQE[Any, Any]]) -> list[CQE[Any]]: ...
    def worker(self) -> None: ...


class AIO(Protocol):
    def attach_subsystem(self, subsystem: Subsystem) -> None: ...
    def start(self) -> None: ...
    def shutdown(self) -> None: ...
    def flush(self, time: int) -> None: ...
    def dispatch(self, sqe: SQE[Any, Any]) -> None: ...
    def dequeue(self, n: int) -> list[CQE[Any]]: ...
    def enqueue(self, cqe: tuple[CQE[Any], str]) -> None: ...


class AIOSystem:
    def __init__(self, size: int) -> None:
        self._cq: queue.Queue[tuple[CQE[Any], str]] = queue.Queue(size)
        self._subsystems: dict[str, Subsystem] = {}

    def attach_subsystem(self, subsystem: Subsystem) -> None:
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

    def dispatch(self, sqe: SQE[Any, Any]) -> None:
        match sqe.v:
            case Callable():
                subsystem = self._subsystems["function"]
            case _:
                subsystem = self._subsystems[sqe.v.kind()]

        if not subsystem.enqueue(sqe):
            sqe.cb(Exception("aio submission queue full"))

    def dequeue(self, n: int) -> list[CQE[Any]]:
        cqes: list[CQE[Any]] = []
        for _ in range(n):
            try:
                cqe, _ = self._cq.get_nowait()
            except queue.Empty:
                break

            cqes.append(cqe)
            self._cq.task_done()
        return cqes

    def enqueue(self, cqe: tuple[CQE[Any], str]) -> None:
        self._cq.put(cqe)


class AIODst:
    def __init__(self, r: Random, p: float) -> None:
        self._r: Random = r
        self._p: float = p
        self._subsystems: dict[str, Subsystem] = {}
        self._sqes: list[SQE[Any, Any]] = []
        self._cqes: list[CQE[Any]] = []
        self._last_flush: int = -1

    def attach_subsystem(self, subsystem: Subsystem) -> None:
        assert subsystem.kind() not in self._subsystems, "subsystem is already registered."
        self._subsystems[subsystem.kind()] = subsystem

    def check(self, value: Kind) -> Any:
        def _(_result: Any | Exception) -> None: ...

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
        assert self._last_flush < time
        self._last_flush = time

        flush: dict[str, list[SQE[Any, Any]]] = {}
        for sqe in self._sqes:
            flush.setdefault(
                sqe.v.kind() if not isinstance(sqe.v, Callable) else "function", []
            ).append(sqe)

        for kind, sqes in flush.items():
            assert kind in self._subsystems, "invalid aio submission"
            to_process: list[SQE[Any, Any]] = []
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
                        case _:
                            msg = "unexpected number generated"
                            raise RuntimeError(msg)

                if pre_failure.get(i, False):
                    self.enqueue(
                        (
                            CQE[Any](Exception("simulated failure before processing"), sqe.cb),
                            "dst",
                        )
                    )
                else:
                    to_process.append(sqe)
                    n += 1

            if len(to_process) == 0:
                continue

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

    def dispatch(self, sqe: SQE[Any, Any]) -> None:
        self._sqes.insert(self._r.randrange(len(self._sqes) + 1), sqe)

    def dequeue(self, n: int) -> list[CQE[Any]]:
        cqes = self._cqes[: min(n, len(self._cqes))]
        self._cqes = self._cqes[min(n, len(self._cqes)) :]
        return cqes

    def enqueue(self, cqe: tuple[CQE[Any], str]) -> None:
        self._cqes.append(cqe[0])
