from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from collections.abc import Callable


class _Kind:
    def kind(self) -> Literal["function"]:
        return "function"


@dataclass(frozen=True)
class FunctionSubmission(_Kind):
    fn: Callable[[], Any]


@dataclass(frozen=True)
class FunctionCompletion(_Kind):
    result: Any | Exception
