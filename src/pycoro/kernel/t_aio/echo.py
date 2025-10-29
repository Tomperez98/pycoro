from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EchoSubmission:
    data: str

    def kind(self) -> str:
        return "echo"


@dataclass(frozen=True)
class EchoCompletion:
    data: str

    def kind(self) -> str:
        return "echo"
