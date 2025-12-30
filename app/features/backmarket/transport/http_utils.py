"""
HTTP helpers for Back Market transport.

Keep these utilities defensive: they are used inside retry/rate-limit paths and
must not raise unexpected exceptions.
"""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Union

import httpx


HeadersLike = Union[httpx.Headers, Mapping[str, str]]
RespOrHeaders = Union[httpx.Response, HeadersLike]


def _as_headers(obj: RespOrHeaders) -> HeadersLike:
    return obj.headers if isinstance(obj, httpx.Response) else obj


def is_cloudflare_503(resp: httpx.Response) -> bool:
    """
    Best-effort detection of Cloudflare "rate limited" 503 pages.

    We treat these similarly to 429.
    """
    if resp.status_code != 503:
        return False

    server = (resp.headers.get("server") or "").lower()
    if "cloudflare" not in server:
        return False

    # Cloudflare responses almost always include cf-ray.
    return bool(resp.headers.get("cf-ray") or resp.headers.get("CF-Ray"))


def parse_retry_after_seconds(obj: RespOrHeaders) -> Optional[float]:
    """
    Parse Retry-After header into seconds.

    Accepts:
      - httpx.Response (common)
      - httpx.Headers (some call sites pass resp.headers)
      - Mapping[str, str]

    Returns:
      float seconds if present+valid, else None.
    """
    headers = _as_headers(obj)
    ra = headers.get("Retry-After") or headers.get("retry-after")
    if ra is None:
        return None

    s = str(ra).strip()
    if not s:
        return None

    try:
        secs = float(s)
    except (TypeError, ValueError):
        return None

    if secs < 0:
        return 0.0
    return secs


def parse_cf_ray(obj: RespOrHeaders) -> Optional[str]:
    """
    Extract Cloudflare cf-ray value (if present).
    """
    headers = _as_headers(obj)
    val = headers.get("cf-ray") or headers.get("CF-Ray") or headers.get("Cf-Ray")
    if val is None:
        return None
    s = str(val).strip()
    return s or None


def safe_text_prefix(resp: httpx.Response, *, limit: int = 500) -> Optional[str]:
    """
    Return a small UTF-8 decoded prefix of the response body (errors replaced).

    Never raises.
    """
    n = int(limit)
    if n <= 0:
        return None

    data = resp.content or b""
    if not data:
        return None

    return data[:n].decode("utf-8", errors="replace")


# Back-compat alias (older code imports this private name).
_safe_text_prefix = safe_text_prefix


def read_response_snippet(resp: httpx.Response, limit: int = 500) -> str:
    """
    A logging-friendly body snippet. Always returns a string (possibly empty).
    """
    return safe_text_prefix(resp, limit=int(limit)) or ""


def redact_headers(headers: Mapping[str, Any]) -> Dict[str, str]:
    """
    Redact sensitive headers for logs.

    Input may be httpx.Headers or a dict-like mapping.
    """
    sensitive = {
        "authorization",
        "proxy-authorization",
        "cookie",
        "set-cookie",
        "x-api-key",
        "x-auth-token",
    }

    out: Dict[str, str] = {}
    for k, v in headers.items():
        key = str(k)
        if key.lower() in sensitive:
            out[key] = "<redacted>"
        else:
            out[key] = str(v)
    return out


