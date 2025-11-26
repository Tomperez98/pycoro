from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol


class ResponsePayload(Protocol):
    def kind(self) -> str: ...
    def is_response_payload(self) -> Literal[True]: ...


@dataclass(frozen=True)
class Response[T: ResponsePayload]:
    status: int
    payload: T

    def kind(self) -> str:
        return self.payload.kind()
