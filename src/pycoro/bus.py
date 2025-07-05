from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(frozen=True)
class SQE:
    value: Any
    callback: Callable[[Any | Exception], None]


@dataclass(frozen=True)
class CQE:
    value: Any | Exception
    callback: Callable[[Any | Exception], None]
