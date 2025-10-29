from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    import queue


class _SubsystemBase(Protocol):
    def kind(self) -> str: ...
    def start(self, q: queue.Queue[Exception] | None) -> None: ...
    def stop(self) -> None: ...


class Subsystem(_SubsystemBase, Protocol):
    def enqueue(self, v: Any) -> bool: ...
    def flush(self, time: int) -> None: ...


class SubsystemDST(_SubsystemBase, Protocol):
    def process(self, sqes: list[Any]) -> list[Any]: ...
