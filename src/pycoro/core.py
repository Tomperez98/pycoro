from __future__ import annotations

import threading
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, NamedTuple

from pycoro.errors import UserError
from pycoro.processor import AsyncProcessor, Processor

if TYPE_CHECKING:
    from collections.abc import Callable


class Packet(NamedTuple):
    id: str
    fn: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    future: Future[Any]


class Pycoro:
    def __init__(self, maxsize: int, processor: Processor | None = None) -> None:
        if maxsize <= 0:
            msg = f"maxsize must be greater than 0. set {maxsize}"
            raise UserError(msg)

        self._maxsize: int = maxsize
        self._cv: threading.Condition = threading.Condition()
        self._write_buffer: list[Packet] = []
        self._read_buffer: list[Packet] = []
        self._in_flight: dict[str, Future[Any]] = {}
        self._processor: Processor = processor if processor is not None else AsyncProcessor()

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
        # 1. Input / Submission Phase
        batch = self._grab_batch()

        # Only process batch if it exists, but DO NOT return early
        if batch is not None:
            for pkt in batch:
                if pkt.future.cancelled():
                    continue

                self._processor.submit(pkt.id, pkt.fn, *pkt.args, **pkt.kwargs)
                assert pkt.id not in self._in_flight, "duplicate id"
                self._in_flight[pkt.id] = pkt.future

            self._processor.flush()

        # 2. Completion / Reaping Phase
        # This must run EVERY tick, regardless of whether we had a batch or not
        for cqe in self._processor.results():
            assert cqe.info.id in self._in_flight, "we received something that it's not tracked"
            fut = self._in_flight.pop(cqe.info.id)
            if isinstance(cqe.result, Exception):
                fut.set_exception(cqe.result)
            else:
                fut.set_result(cqe.result)
