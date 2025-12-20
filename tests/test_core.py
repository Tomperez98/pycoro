from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any

from pycoro.core import Pycoro

if TYPE_CHECKING:
    from concurrent.futures import Future


def test_concurrent_processing() -> None:
    pycoro = Pycoro(maxsize=5)
    pycoro.start()
    results: list[Future[Any]] = []

    event = threading.Event()

    def producer(core: Pycoro, results: list[Future[Any]], event: threading.Event) -> None:
        for i in range(5):
            # Slow down producers to simulate real-world flow
            f = core.add(f"id_{i}", lambda x: x + 1, i)
            results.append(f)

        event.set()

    # Start producer thread
    p_thread = threading.Thread(target=producer, args=(pycoro, results, event), daemon=True)
    p_thread.start()

    # Wait for producers to fill buffer
    event.wait()

    # Process everything
    pycoro.tick()
    p_thread.join()

    # Verify results
    processed_values = [f.result() for f in results]
    assert processed_values == [1, 2, 3, 4, 5]
