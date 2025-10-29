from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class SubmissionPayload(Protocol):
    def kind(self) -> str: ...


@dataclass(frozen=True)
class Submission[T: SubmissionPayload]:
    payload: T
    tags: dict[str, str]

    def kind(self) -> str:
        return self.kind()


class CompletionPayload(Protocol):
    def kind(self) -> str: ...


@dataclass(frozen=True)
class Completion[T: CompletionPayload]:
    payload: T
    tags: dict[str, str]

    def kind(self) -> str:
        return self.payload.kind()
