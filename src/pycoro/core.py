from __future__ import annotations

import threading
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, NamedTuple

from pycoro.errors import UserError
from pycoro.processor import Processor

if TYPE_CHECKING:
    from collections.abc import Callable


class Packet(NamedTuple):
    id: str
    fn: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    future: Future[Any]


class Pycoro:
    def __init__(self, maxsize: int) -> None:
        if maxsize <= 0:
            msg = f"maxsize must be greater than 0. set {maxsize}"
            raise UserError(msg)

        self._maxsize: int = maxsize
        self._cv: threading.Condition = threading.Condition()
        self._write_buffer: list[Packet] = []
        self._read_buffer: list[Packet] = []
        self._processor: Processor = Processor()

    def start(self) -> None:
        self._processor.start()

    def stop(self) -> None:
        self._processor.stop()
        # Stop all blocked adds waiting for the condition

    def add[**P, T](
        self, id: str, fn: Callable[P, T], *args: P.args, **kwargs: P.kwargs
    ) -> Future[T]:
        fut = Future[T]()
        pkt = Packet(id, fn, args, kwargs, fut)

        with self._cv:
            # wait_for returns False if the timeout expires
            self._cv.wait_for(
                lambda: len(self._write_buffer) < self._maxsize,
            )
            self._write_buffer.append(pkt)

        return fut

    def _grab_batch(self) -> list[Packet] | None:
        with self._cv:
            if not self._write_buffer:
                return None

            self._write_buffer, self._read_buffer = self._read_buffer, self._write_buffer

            self._cv.notify_all()

        return self._read_buffer

    def tick(self) -> None:
        batch = self._grab_batch()
        if not batch:
            return

        batch_futures: dict[str, Future[Any]] = {}
        for pkt in batch:
            if pkt.future.cancelled():
                continue

            self._processor.submit(pkt.id, pkt.fn, *pkt.args, **pkt.kwargs)
            assert pkt.id not in batch_futures, "duplicate id"
            batch_futures[pkt.id] = pkt.future

        self._processor.flush()

        for cqe in self._processor.results():
            fut = batch_futures.pop(cqe.info.id)
            if isinstance(cqe.result, Exception):
                fut.set_exception(cqe.result)
            else:
                fut.set_result(cqe.result)
