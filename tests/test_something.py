from __future__ import annotations

import random
from typing import Any

from pycoro import CQE, Processor


def test_processor() -> None:
    p = Processor(max_workers=1)
    p.start()
    num_tasks = 10
    expected_results: dict[str, int] = {}
    for i in range(num_tasks):
        taskid = f"task::{i}"
        val1 = random.randint(1, 100)
        val2 = random.randint(1, 100)
        assert taskid not in expected_results
        expected_results[taskid] = sum((val1, val2))
        p.submit(
            taskid,
            lambda *nums: sum(nums),
            val1,
            val2,
        )

    results: list[CQE[Any]] = []
    for _ in range(num_tasks):
        res = p.wait_for_value()
        assert sum(res.info.args) == res.result == expected_results.pop(res.info.id)
        results.append(res)

    assert len(results) == num_tasks
    p.stop()
