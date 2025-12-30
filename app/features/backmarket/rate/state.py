"""Rate state model.

`EndpointRateState` is the persisted + cached adaptive rate state for a
(user_id, endpoint_key) pair.

This module is intentionally dependency-free (no Motor/PyMongo) so it can be
used by the learner and transport without bringing in DB concerns.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class EndpointConfig:
    """Static config for an endpoint."""

    base_rps: int
    min_rps: int = 1
    max_rps: int = 50


def _as_int(v: Any, default: int) -> int:
    if v is None:
        return int(default)
    try:
        return int(v)
    except (TypeError, ValueError):
        return int(default)


def _as_float(v: Any, default: float) -> float:
    if v is None:
        return float(default)
    try:
        f = float(v)
    except (TypeError, ValueError):
        return float(default)

    # Avoid NaN/inf poisoning the state.
    if not math.isfinite(f):
        return float(default)
    return f


def _as_dt(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        # Normalize to tz-aware UTC.
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc)
    if isinstance(v, str):
        try:
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    return None


@dataclass
class EndpointRateState:
    user_id: str
    endpoint_key: str

    base_rps: int
    min_rps: int
    max_rps: int

    # Learned value (float so we can do small deltas).
    rps_float: float

    # Integer value applied to the limiter.
    current_rps: int

    # When we have observed rate-limiting, we cap exploration.
    locked_rps: Optional[float] = None

    # During cooldown, we do not step up.
    cooldown_until: Optional[datetime] = None

    consecutive_successes: int = 0
    consecutive_429s: int = 0

    success_count: int = 0
    error_count: int = 0

    last_status: Optional[int] = None

    updated_at: Optional[datetime] = None

    def in_cooldown(self, *, now: Optional[datetime] = None) -> bool:
        """Return True if we are currently in cooldown."""

        if self.cooldown_until is None:
            return False
        if now is None:
            now = datetime.now(timezone.utc)
        return now < self.cooldown_until

    def set_cooldown(self, seconds: float, *, now: Optional[datetime] = None) -> None:
        """Set/extend cooldown window.

        If a cooldown is already in effect, we keep the second of the existing items
        and the newly requested cooldown.
        """

        if now is None:
            now = datetime.now(timezone.utc)

        try:
            s = float(seconds)
        except (TypeError, ValueError):
            s = 0.0

        if not math.isfinite(s) or s <= 0:
            self.cooldown_until = None
            return

        until = now + timedelta(seconds=s)
        if self.cooldown_until is None or until > self.cooldown_until:
            self.cooldown_until = until

    def clamp_and_recompute(self) -> None:
        """Clamp floats, derive `current_rps`, and normalize fields."""

        base_rps_f = float(self.base_rps)
        min_rps_f = float(self.min_rps)
        max_rps_f = float(self.max_rps)

        # Normalize rps_float.
        self.rps_float = _as_float(self.rps_float, base_rps_f)
        self.rps_float = max(min_rps_f, min(max_rps_f, float(self.rps_float)))

        # Normalize locked_rps.
        if self.locked_rps is not None:
            lr = _as_float(self.locked_rps, float(self.rps_float))
            lr = max(min_rps_f, min(max_rps_f, lr))
            self.locked_rps = lr

            # Apply ceiling.
            self.rps_float = min(float(self.rps_float), float(lr))
            # Ensure we don't fall below min.
            self.rps_float = max(float(self.rps_float), min_rps_f)

        # Derive integer RPS from rps_float.
        # We avoid Python's banker's rounding (round(4.5) == 4). Instead,
        # we use "round half up" via floor(x + 0.5).
        current = int(math.floor(float(self.rps_float) + 0.5))
        current = max(self.min_rps, min(self.max_rps, current))

        self.current_rps = int(current)

        # Normalize updated_at.
        if self.updated_at is None:
            self.updated_at = datetime.now(timezone.utc)
        elif self.updated_at.tzinfo is None:
            self.updated_at = self.updated_at.replace(tzinfo=timezone.utc)
        else:
            self.updated_at = self.updated_at.astimezone(timezone.utc)

        # Normalize cooldown_until.
        if self.cooldown_until is not None:
            if self.cooldown_until.tzinfo is None:
                self.cooldown_until = self.cooldown_until.replace(tzinfo=timezone.utc)
            else:
                self.cooldown_until = self.cooldown_until.astimezone(timezone.utc)

    def to_mongo(self) -> Dict[str, Any]:
        return {
            "user_id": self.user_id,
            "endpoint_key": self.endpoint_key,
            "base_rps": int(self.base_rps),
            "min_rps": int(self.min_rps),
            "max_rps": int(self.max_rps),
            "rps_float": float(self.rps_float),
            "current_rps": int(self.current_rps),
            "locked_rps": float(self.locked_rps) if self.locked_rps is not None else None,
            "cooldown_until": self.cooldown_until,
            "consecutive_successes": int(self.consecutive_successes),
            "consecutive_429s": int(self.consecutive_429s),
            "success_count": int(self.success_count),
            "error_count": int(self.error_count),
            "last_status": int(self.last_status) if self.last_status is not None else None,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_mongo(
        cls, user_id: str, endpoint_key: str, doc: Dict[str, Any], cfg: EndpointConfig
    ) -> "EndpointRateState":
        state = cls(
            user_id=user_id,
            endpoint_key=endpoint_key,
            base_rps=_as_int(doc.get("base_rps"), cfg.base_rps),
            min_rps=_as_int(doc.get("min_rps"), cfg.min_rps),
            max_rps=_as_int(doc.get("max_rps"), cfg.max_rps),
            rps_float=_as_float(doc.get("rps_float"), float(cfg.base_rps)),
            current_rps=_as_int(doc.get("current_rps"), int(cfg.base_rps)),
            locked_rps=(
                _as_float(doc.get("locked_rps"), float(cfg.base_rps))
                if doc.get("locked_rps") is not None
                else None
            ),
            cooldown_until=_as_dt(doc.get("cooldown_until")),
            consecutive_successes=_as_int(doc.get("consecutive_successes"), 0),
            consecutive_429s=_as_int(doc.get("consecutive_429s"), 0),
            success_count=_as_int(doc.get("success_count"), 0),
            error_count=_as_int(doc.get("error_count"), 0),
            last_status=_as_int(doc.get("last_status"), 0) if doc.get("last_status") is not None else None,
            updated_at=_as_dt(doc.get("updated_at")) or datetime.now(timezone.utc),
        )
        state.clamp_and_recompute()
        return state
