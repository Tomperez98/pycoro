from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pycoro.errors import UserError

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(frozen=True)
class Run:
    fn: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]

    id: str | None = None
    options_set: bool = field(default=False, repr=False)

    def options(self, id: str | None = None) -> Run:
        if self.options_set:
            msg = "options can be set at most once."
            raise UserError(msg)

        return Run(
            fn=self.fn,
            args=self.args,
            kwargs=self.kwargs,
            id=id,
            options_set=not self.options_set,
        )


type Yieldable = Run


@dataclass(frozen=True)
class P[T]:
    id: str


type Sendable[T] = P[T] | T | Exception
