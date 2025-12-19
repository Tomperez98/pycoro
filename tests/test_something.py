from __future__ import annotations

import random
from typing import Any

from pycoro import CQE, Processor


def test_processor() -> None:
    p = Processor(max_workers=1)
    p.start()
    num_tasks = 10
    for i in range(num_tasks):
        p.submit(
            f"task::{i}", lambda *nums: sum(nums), random.randint(1, 100), random.randint(1, 100)
        )

    results: list[CQE[Any]] = []
    for _ in range(num_tasks):
        res = p.wait_for_value()
        assert sum(res.args) == res.result
        results.append(res)

    assert len(results) == num_tasks
