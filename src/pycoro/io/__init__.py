from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Callable


class IO[T](Protocol):
    def dispatch(self, value: Any, callback: Callable[[T | Exception], None]) -> None: ...
    def shutdown(self) -> None: ...
