from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    url: str
    coroutine_max_size: int
    submission_batch_size: int
    completion_batch_size: int
    future_batch_size: int
    schedule_batch_size: int
