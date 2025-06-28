from __future__ import annotations

import time
from typing import TYPE_CHECKING

from pycoro.scheduler import Computation, Handle, Scheduler

if TYPE_CHECKING:
    from pycoro.io import IO


class Pycoro[I, O]:
    def __init__(self, io: IO[I, O], size: int, dequeue_size: int) -> None:
        self._io = io
        self._scheduler = Scheduler(self._io, size)
        self._dequeue_size = dequeue_size

    def loop(self) -> None:
        while True:
            self.tick(int(time.time() * 1_000))

            if self.done():
                self.shutdown()
                return

    def tick(self, time: int) -> None:
        for cqe in self._io.dequeue(self._dequeue_size):
            cqe.callback(cqe.value)

        self._scheduler.run_until_blocked(time)
        self._io.flush(time)

    def done(self) -> bool:
        return self._scheduler.size() == 0

    def shutdown(self) -> None:
        self._scheduler.shutdown()

    def add(self, c: Computation[I, O] | I) -> Handle[O]:
        return self._scheduler.add(c)

    def start(self) -> None:
        self._io.start()
