from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from queue import Empty, SimpleQueue
from typing import TYPE_CHECKING, Any

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


class Processor:
    def __init__(self, max_workers: int | None = None) -> None:
        self._max_workers: int | None = max_workers
        self._pool: ThreadPoolExecutor | None = None
        self._cq: SimpleQueue[CQE[Any]] = SimpleQueue()
        self._sq: list[SQE[Any]] = []

    def submit[**P](self, id: str, fn: Callable[P, Any], *args: P.args, **kwargs: P.kwargs) -> None:
        self._sq.append(SQE(id=id, fn=fn, args=args, kwargs=kwargs))

    def flush(self) -> int:
        assert self._pool is not None, "processor was never started"
        count = len(self._sq)
        for cqe in self._pool.map(_run_sqe, self._sq):
            self._cq.put(cqe)

        self._sq.clear()
        return count

    def wait_for_batch(self, count: int, timeout: float | None = None) -> list[CQE[Any]]:
        results: list[CQE[Any]] = []
        try:
            first = self._cq.get(timeout=timeout)
            results.append(first)

            for _ in range(count - 1):
                try:
                    results.append(self._cq.get_nowait())
                except Empty:
                    break
        except Empty:
            pass

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
