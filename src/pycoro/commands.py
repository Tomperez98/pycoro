from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(frozen=True)
class Run:
    id: str
    fn: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


type Yieldable = Run


@dataclass(frozen=True)
class P[T]: ...


type Sendable[T] = P[T] | T | Exception
