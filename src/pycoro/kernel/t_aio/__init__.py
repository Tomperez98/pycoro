from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pycoro.kernel.t_aio.echo import EchoCompletion, EchoSubmission
    from pycoro.kernel.t_aio.function import FunctionCompletion, FunctionSubmission
    from pycoro.kernel.t_aio.store import StoreCompletion, StoreSubmission


@dataclass(frozen=True)
class Submission:
    tags: dict[str, str]
    value: EchoSubmission | StoreSubmission | FunctionSubmission


@dataclass(frozen=True)
class Completion:
    tags: dict[str, str]
    value: EchoCompletion | StoreCompletion | FunctionCompletion
