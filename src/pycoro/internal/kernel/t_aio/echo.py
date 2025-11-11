from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


class _Kind:
    def kind(self) -> Literal["echo"]:
        return "echo"


@dataclass(frozen=True)
class EchoSubmission(_Kind):
    data: str


@dataclass(frozen=True)
class EchoCompletion(_Kind):
    data: str
