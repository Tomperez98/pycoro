from __future__ import annotations

import queue
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(frozen=True)
class Info:
    id: str
    fn_name: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass(frozen=True)
class CQE[T]:
    info: Info
    result: Any | Exception


@dataclass(frozen=True)
class SQE[T]:
    id: str
    fn: Callable[..., T]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


class Processor(Protocol):
    def submit[**P](
        self, id: str, fn: Callable[P, Any], *args: P.args, **kwargs: P.kwargs
    ) -> None: ...
    def flush(self) -> None: ...
    def results(self) -> list[CQE[Any]]: ...
    def start(self) -> None: ...
    def stop(self) -> None: ...


class SyncProcessor:
    def __init__(self) -> None:
        self._cq: list[CQE[Any]] = []
        self._sq: list[SQE[Any]] = []

    def submit[**P](self, id: str, fn: Callable[P, Any], *args: P.args, **kwargs: P.kwargs) -> None:
        self._sq.append(SQE(id=id, fn=fn, args=args, kwargs=kwargs))

    def flush(self) -> None:
        assert len(self._cq) == 0, "pending elements on cq"
        for sqe in self._sq:
            self._cq.append(_run_sqe(sqe))

        self._sq.clear()

    def results(self) -> list[CQE[Any]]:
        results = self._cq
        self._cq = []
        return results

    def start(self) -> None:
        return

    def stop(self) -> None:
        return


class AsyncProcessor:
    def __init__(self, max_workers: int | None = None) -> None:
        self._max_workers: int | None = max_workers
        self._pool: ThreadPoolExecutor | None = None
        self._cq: queue.SimpleQueue[CQE[Any]] = queue.SimpleQueue()
        self._sq: list[SQE[Any]] = []

    def submit[**P](self, id: str, fn: Callable[P, Any], *args: P.args, **kwargs: P.kwargs) -> None:
        self._sq.append(SQE(id=id, fn=fn, args=args, kwargs=kwargs))

    def flush(self) -> None:
        assert self._pool is not None, "processor was never started"
        for sqe in self._sq:
            self._pool.submit(_run_sqe, sqe).add_done_callback(lambda f: self._cq.put(f.result()))
        self._sq.clear()

    def results(self) -> list[CQE[Any]]:
        assert self._cq is not None
        results = []

        while True:
            try:
                results.append(self._cq.get_nowait())
            except queue.Empty:
                break

        return results

    def start(self) -> None:
        assert self._pool is None, "processor has already been started"
        self._pool = ThreadPoolExecutor(
            thread_name_prefix="processor::",
            max_workers=self._max_workers,
        )

    def stop(self) -> None:
        assert self._pool is not None, "processor was never started"
        self._pool.shutdown()
        self._pool = None


def _run_sqe[T](sqe: SQE[T]) -> CQE[T]:
    result: T | Exception
    try:
        result = sqe.fn(*sqe.args, **sqe.kwargs)
    except Exception as e:
        result = e

    return CQE(
        Info(
            id=sqe.id,
            fn_name=getattr(sqe.fn, "__name__", "unknown"),
            args=sqe.args,
            kwargs=sqe.kwargs,
        ),
        result=result,
    )
