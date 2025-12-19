from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from pycoro.reproducer import GenIterator, Run, Sendable, Yieldable

if TYPE_CHECKING:
    from collections.abc import Generator


def test_gen_reproducer() -> None:
    iterator = GenIterator()

    # A dummy task function
    def task(name: str) -> str:
        return f"result_{name}"

    # 1. Define the generator factory
    def workflow() -> Generator[Yieldable, Sendable[Any], str]:
        val1 = yield Run(id="step1", fn=task, args=("a",), kwargs={})
        val2 = yield Run(id="step2", fn=task, args=("b",), kwargs={})
        return f"{val1}_{val2}"

    # --- SCENARIO 1: Empty Cache ---
    # Should return the first run
    run1 = iterator.next(workflow())
    assert run1.id == "step1"

    # --- SCENARIO 2: Partial Cache (Successful Send) ---
    iterator.add_to_cache("step1", "data1")

    # Calling next(workflow()) again iterates from top,
    # sends "data1" into step1, and should return step2.
    run2 = iterator.next(workflow())
    assert run2.id == "step2"

    # --- SCENARIO 3: Exception Handling ---
    # Clear and test exception injection
    iterator = GenIterator()
    iterator.add_to_cache("step1", ValueError("fail"))

    def error_workflow() -> Generator[Yieldable, Sendable[Any], str]:
        try:
            yield Run(id="step1", fn=task, args=(), kwargs={})
        except ValueError as e:
            # If the iterator correctly uses gen.throw(), we land here
            yield Run(id="recovery", fn=task, args=(str(e),), kwargs={})

        return "done"

    run_recovery = iterator.next(error_workflow())
    assert run_recovery.id == "recovery"
    assert run_recovery.args == ("fail",)

    # --- SCENARIO 4: Completion ---
    iterator = GenIterator()
    iterator.add_to_cache("step1", "done1")
    iterator.add_to_cache("step2", "done2")

    # All steps cached; the generator should hit StopIteration
    with pytest.raises(StopIteration) as exc_info:
        iterator.next(workflow())

    assert exc_info.value.value == "done1_done2"
