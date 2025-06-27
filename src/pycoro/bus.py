from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(frozen=True)
class SQE[I, O]:
    value: I
    callback: Callable[[O | Exception], None]


@dataclass(frozen=True)
class CQE[O]:
    value: O | Exception
    callback: Callable[[O | Exception], None]
