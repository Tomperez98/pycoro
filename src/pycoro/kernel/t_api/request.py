from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol


class RequestPayload(Protocol):
    def kind(self) -> str: ...
    def validate(self) -> None: ...
    def is_request_payload(self) -> Literal[True]: ...


@dataclass(frozen=True)
class Request:
    metadata: dict[str, str]
    payload: RequestPayload

    def kind(self) -> str:
        return self.payload.kind()

    def validate(self) -> None:
        return self.payload.validate()
