from __future__ import annotations

from enum import Enum


class JobMode(str, Enum):
    AS_IS = "as_is"
    RELEASE = "release"


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
