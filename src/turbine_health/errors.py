"""服务层可观察错误。"""

from __future__ import annotations

from typing import Any, Mapping


class ServiceError(RuntimeError):
    code = "service_error"
    status = 400
    details: Mapping[str, Any] | None = None


class NotFound(ServiceError):
    code = "not_found"
    status = 404


class Conflict(ServiceError):
    code = "conflict"
    status = 409


class LeaseConflict(Conflict):
    """分析任务提交栅栏失配：租约已落后，未产生任何写入。"""

    code = "lease_conflict"
    status = 409

    def __init__(self, message: str, *, details: Mapping[str, Any]) -> None:
        super().__init__(message)
        self.details = details


class Forbidden(ServiceError):
    code = "forbidden"
    status = 403


class InvalidState(ServiceError):
    code = "invalid_state"
    status = 409


class ValidationFailed(ServiceError):
    code = "validation_failed"
    status = 422
