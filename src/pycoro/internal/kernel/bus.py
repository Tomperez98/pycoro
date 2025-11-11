from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pycoro.internal.kernel import t_aio, t_api
from pycoro.internal.typing import Kind

if TYPE_CHECKING:
    from collections.abc import Callable


type Input = t_aio.Submission[Kind] | t_api.Request
type Output = t_aio.Completion[Kind] | t_api.Response


@dataclass(frozen=True)
class SQE[I: Input, O: Output]:
    id: str | None
    callback: Callable[[O | Exception], None]
    submission: I


@dataclass
class CQE[I: Input, O: Output]:
    id: str | None
    callback: Callable[[O | Exception], None]
    completion: O | Exception

    def invoke(self) -> None:
        return self.callback(self.completion)
