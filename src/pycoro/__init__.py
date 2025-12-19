from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from queue import Queue
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(frozen=True)
class CQE[T]:
    id: str
    fn_name: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    result: Any | Exception


class Processor:
    def __init__(self, max_workers: int | None = None) -> None:
        self._pool: ThreadPoolExecutor = ThreadPoolExecutor(
            thread_name_prefix="processor::", max_workers=max_workers
        )
        self._cq: Queue[CQE[Any]] = Queue()
        self._in_flight: int = 0

    def submit[**P](self, id: str, fn: Callable[P, Any], *args: P.args, **kwargs: P.kwargs) -> None:
        def _(f: Future[Any]) -> None:
            assert f.done(), "this should be executed at the done callback"
            v: Any | Exception
            try:
                v = f.result()
            except Exception as e:
                v = e
            self._cq.put(
                CQE(
                    id=id,
                    fn_name=getattr(fn, "__name__", "unknown"),
                    args=args,
                    kwargs=kwargs,
                    result=v,
                )
            )

        self._pool.submit(fn, *args, **kwargs).add_done_callback(_)
        self._in_flight += 1

    def wait_for_value(self) -> CQE[Any]:
        assert self._in_flight > 0, "No tasks in flight to wait for."
        v = self._cq.get()
        self._in_flight -= 1
        return v

    def start(self) -> None: ...

    def stop(self) -> None:
        self._pool.shutdown()
