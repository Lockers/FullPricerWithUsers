# app/core/errors.py
"""
Centralised error types + FastAPI exception handlers.

(unchanged docs above)
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pymongo.errors import DuplicateKeyError
from starlette import status

logger = logging.getLogger(__name__)


class AppError(Exception):
    def __init__(
        self,
        *,
        status_code: int,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = int(status_code)
        self.code = str(code)
        self.message = str(message)
        self.details = details
        self.headers = headers


class NotFoundError(AppError):
    def __init__(self, *, code: str = "not_found", message: str = "Not found", details: dict[str, Any] | None = None):
        super().__init__(status_code=status.HTTP_404_NOT_FOUND, code=code, message=message, details=details)


class ConflictError(AppError):
    def __init__(self, *, code: str = "conflict", message: str = "Conflict", details: dict[str, Any] | None = None):
        super().__init__(status_code=status.HTTP_409_CONFLICT, code=code, message=message, details=details)


class BadRequestError(AppError):
    def __init__(self, *, code: str = "bad_request", message: str = "Bad request", details: dict[str, Any] | None = None):
        super().__init__(status_code=status.HTTP_400_BAD_REQUEST, code=code, message=message, details=details)


class UnauthorizedError(AppError):
    def __init__(self, *, code: str = "unauthorized", message: str = "Unauthorized", details: dict[str, Any] | None = None):
        super().__init__(status_code=status.HTTP_401_UNAUTHORIZED, code=code, message=message, details=details)


class ForbiddenError(AppError):
    def __init__(self, *, code: str = "forbidden", message: str = "Forbidden", details: dict[str, Any] | None = None):
        super().__init__(status_code=status.HTTP_403_FORBIDDEN, code=code, message=message, details=details)


def _request_id(request: Request) -> str | None:
    rid = getattr(request.state, "request_id", None)
    if isinstance(rid, str) and rid.strip():
        return rid.strip()

    hdr = request.headers.get("x-request-id")
    if hdr and hdr.strip():
        return hdr.strip()

    return None


def _error_payload(*, code: str, message: str, details: dict[str, Any] | None, request_id: str | None) -> dict[str, Any]:
    return {
        "error": {
            "code": code,
            "message": message,
            "details": details,
        },
        "request_id": request_id,
    }


def _json_error(
    request: Request,
    *,
    status_code: int,
    code: str,
    message: str,
    details: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    rid = _request_id(request)

    out_headers = dict(headers or {})
    if rid:
        out_headers.setdefault("X-Request-ID", rid)

    return JSONResponse(
        status_code=int(status_code),
        content=_error_payload(code=code, message=message, details=details, request_id=rid),
        headers=out_headers,
    )


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppError)
    async def _handle_app_error(request: Request, exc: AppError) -> JSONResponse:
        return _json_error(
            request,
            status_code=exc.status_code,
            code=exc.code,
            message=exc.message,
            details=exc.details,
            headers=exc.headers,
        )

    @app.exception_handler(RequestValidationError)
    async def _handle_validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        return _json_error(
            request,
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            code="validation_error",
            message="Request validation failed",
            details={"errors": exc.errors()},
        )

    @app.exception_handler(HTTPException)
    async def _handle_http_exception(request: Request, exc: HTTPException) -> JSONResponse:
        details: dict[str, Any] | None = None
        message = "Request failed"

        if isinstance(exc.detail, str):
            message = exc.detail
        elif isinstance(exc.detail, dict):
            message = str(exc.detail.get("message") or message)
            details = exc.detail

        return _json_error(
            request,
            status_code=exc.status_code,
            code="http_exception",
            message=message,
            details=details,
            headers=getattr(exc, "headers", None),
        )

    @app.exception_handler(DuplicateKeyError)
    async def _handle_duplicate_key(request: Request, _exc: DuplicateKeyError) -> JSONResponse:
        logger.info("DuplicateKeyError")
        return _json_error(
            request,
            status_code=status.HTTP_409_CONFLICT,
            code="duplicate_key",
            message="A record with the same unique key already exists",
        )

    # ------------------------------------------------------------------
    # Optional BackMarket exception mapping
    #
    # Import the optional module and only register handlers if available.
    # Ensures:
    # - bm_exceptions is defined on both try/except paths
    # - no CamelCase local variables
    # ------------------------------------------------------------------

    try:
        from app.features.backmarket.transport import exceptions as bm_exceptions  # noqa: WPS433
    except ImportError:
        bm_exceptions = None
        logger.debug("BackMarket exceptions not importable; skipping BM exception handlers")

    if bm_exceptions is not None:

        @app.exception_handler(bm_exceptions.BMMisconfiguredError)
        async def _handle_bm_misconfigured(request: Request, _exc: Exception) -> JSONResponse:
            return _json_error(
                request,
                status_code=status.HTTP_400_BAD_REQUEST,
                code="bm_misconfigured",
                message="Back Market credentials are missing or invalid for this user",
            )

        @app.exception_handler(bm_exceptions.BMRateLimited)
        async def _handle_bm_rate_limited(request: Request, _exc: Exception) -> JSONResponse:
            return _json_error(
                request,
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                code="bm_rate_limited",
                message="Temporarily rate-limited while calling Back Market; retry shortly",
            )

        @app.exception_handler(bm_exceptions.BMMaxRetriesError)
        async def _handle_bm_max_retries(request: Request, _exc: Exception) -> JSONResponse:
            return _json_error(
                request,
                status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                code="bm_timeout",
                message="Back Market did not respond after multiple retries",
            )

        @app.exception_handler(bm_exceptions.BMClientError)
        async def _handle_bm_client_error(request: Request, _exc: Exception) -> JSONResponse:
            return _json_error(
                request,
                status_code=status.HTTP_502_BAD_GATEWAY,
                code="bm_upstream_error",
                message="Back Market request failed",
            )

    @app.exception_handler(Exception)
    async def _handle_unexpected(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("Unhandled exception", exc_info=exc)
        return _json_error(
            request,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            code="internal_error",
            message="Internal server error",
        )







