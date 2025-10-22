from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Callable


class IO[I, O](Protocol):
    def dispatch(self, v: I | None, cb: Callable[[O | Exception], None]) -> None: ...
