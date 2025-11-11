from __future__ import annotations

from enum import IntEnum
from typing import override

MIN_SUCCESS_RANGE = 20000
MAX_SUCCESS_RANGE = 30000


class StatusCode(IntEnum):
    # Application level status (20000-49999)
    STATUS_OK = 20000
    STATUS_CREATED = 20100
    STATUS_NO_CONTENT = 20400

    STATUS_FIELD_VALIDATION_ERROR = 40000

    # Platform level status (50000-59999)
    STATUS_INTERNAL_SERVER_ERROR = 50000
    STATUS_AIO_ECHO_ERROR = 50001
    STATUS_AIO_MATCH_ERROR = 50002
    STATUS_AIO_QUEUE_ERROR = 50003
    STATUS_AIO_STORE_ERROR = 50004
    STATUS_SYSTEM_SHUTTING_DOWN = 50300
    STATUS_API_SUBMISSION_QUEUE_FULL = 50301
    STATUS_AIO_SUBMISSION_QUEUE_FULL = 50302
    STATUS_SCHEDULER_QUEUE_FULL = 50303

    @override
    def __str__(self) -> str:
        messages = {
            self.STATUS_OK: "The request was successful",
            self.STATUS_CREATED: "The request was successful",
            self.STATUS_NO_CONTENT: "The request was successful",
            self.STATUS_FIELD_VALIDATION_ERROR: "The request is invalid",
            self.STATUS_INTERNAL_SERVER_ERROR: "There was an internal server error",
            self.STATUS_AIO_ECHO_ERROR: "There was an error in the echo subsystem",
            self.STATUS_AIO_MATCH_ERROR: "There was an error in the match subsystem",
            self.STATUS_AIO_QUEUE_ERROR: "There was an error in the queue subsystem",
            self.STATUS_AIO_STORE_ERROR: "There was an error in the store subsystem",
            self.STATUS_SYSTEM_SHUTTING_DOWN: "The system is shutting down",
            self.STATUS_API_SUBMISSION_QUEUE_FULL: "The api submission queue is full",
            self.STATUS_AIO_SUBMISSION_QUEUE_FULL: "The aio submission queue is full",
            self.STATUS_SCHEDULER_QUEUE_FULL: "The scheduler queue is full",
        }
        try:
            return messages[self]
        except KeyError as e:
            msg = f"Unknown status code {int(self)}"
            raise ValueError(msg) from e

    def is_successful(self) -> bool:
        return MIN_SUCCESS_RANGE <= self.value < MAX_SUCCESS_RANGE
