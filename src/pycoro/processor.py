from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from queue import Queue
from typing import TYPE_CHECKING, Any
from uuid import uuid4

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


class Processor:
    def __init__(self, max_workers: int | None = None) -> None:
        self._max_workers: int | None = max_workers
        self._pool: ThreadPoolExecutor | None = None
        self._cq: Queue[CQE[Any]] = Queue()
        self._in_flight: int = 0

    def submit[**P](self, fn: Callable[P, Any], *args: P.args, **kwargs: P.kwargs) -> str:
        taskid = uuid4().hex
        assert self._pool is not None, "processor was never started"
        self._pool.submit(fn, *args, **kwargs).add_done_callback(
            lambda f: self._cq.put(
                CQE(
                    Info(
                        id=taskid,
                        fn_name=getattr(fn, "__name__", "unknown"),
                        args=args,
                        kwargs=kwargs,
                    ),
                    result=f.exception() or f.result(),
                )
            )
        )
        self._in_flight += 1
        return taskid

    def wait_for_value(self) -> CQE[Any]:
        assert self._in_flight > 0, "No tasks in flight to wait for."
        v = self._cq.get()
        self._in_flight -= 1
        return v

    def start(self) -> None:
        assert self._pool is None, "processor has already been started"
        self._pool = ThreadPoolExecutor(
            thread_name_prefix="processor::",
            max_workers=self._max_workers,
        )

    def stop(self) -> None:
        assert self._pool is not None, "processor was never started"
        self._pool.shutdown()
