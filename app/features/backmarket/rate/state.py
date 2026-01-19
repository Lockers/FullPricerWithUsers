"""Rate state model.

`EndpointRateState` is the persisted + cached adaptive rate state for a
(user_id, endpoint_key) pair.

This module is intentionally dependency-free (no Motor/PyMongo) so it can be
used by the learner and transport without bringing in DB concerns.

Learned dimensions
------------------
Back Market / Cloudflare rate-limiting tends to be sensitive to **both**:
- request rate (RPS)
- request concurrency (in-flight requests)

We therefore persist + tune both.

Fractional RPS
--------------
Some endpoints appear to have thresholds below 1 request/sec. We store and
apply RPS as a float (e.g. 0.5, 0.75).
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional


# Safety floor: never allow <= 0 rps, or the pipeline would stall indefinitely.
ABSOLUTE_MIN_RPS = 0.01


@dataclass(frozen=True)
class EndpointConfig:
    """Static config for an endpoint."""

    # Requests / second
    base_rps: float
    min_rps: float = 1.0
    max_rps: float = 50.0

    # In-flight request concurrency
    base_concurrency: int = 1
    min_concurrency: int = 1
    max_concurrency: int = 20


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
    return float(f)


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


def _clamp_float(v: float, lo: float, hi: float) -> float:
    return max(float(lo), min(float(hi), float(v)))


@dataclass
class EndpointRateState:
    user_id: str
    endpoint_key: str

    # ---------------------------
    # Static config (persisted)
    # ---------------------------
    base_rps: float
    min_rps: float
    max_rps: float

    base_concurrency: int
    min_concurrency: int
    max_concurrency: int

    # ---------------------------
    # Learned values
    # ---------------------------
    # Floats so the learner can use small deltas.
    rps_float: float
    concurrency_float: float

    # Values applied to limiters.
    current_rps: float
    current_concurrency: int

    # When we have observed rate-limiting, we cap exploration.
    locked_rps: Optional[float] = None
    locked_concurrency: Optional[float] = None

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

        If a cooldown is already in effect, we keep the later of the existing
        and newly requested cooldowns.
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
        """Clamp floats, derive applied fields, and normalize timestamps."""

        # ---- RPS ----
        base_rps_f = _as_float(self.base_rps, 1.0)
        min_rps_f = _as_float(self.min_rps, 1.0)
        max_rps_f = _as_float(self.max_rps, 50.0)

        # Normalize invariants + apply absolute safety floor.
        min_rps_f = max(float(ABSOLUTE_MIN_RPS), float(min_rps_f))
        max_rps_f = max(float(min_rps_f), float(max_rps_f))
        base_rps_f = _clamp_float(base_rps_f, min_rps_f, max_rps_f)

        self.base_rps = base_rps_f
        self.min_rps = min_rps_f
        self.max_rps = max_rps_f

        self.rps_float = _as_float(self.rps_float, base_rps_f)
        self.rps_float = _clamp_float(self.rps_float, min_rps_f, max_rps_f)

        if self.locked_rps is not None:
            lr = _as_float(self.locked_rps, float(self.rps_float))
            lr = _clamp_float(lr, min_rps_f, max_rps_f)
            self.locked_rps = lr
            # Apply ceiling.
            self.rps_float = min(float(self.rps_float), float(lr))
            self.rps_float = max(float(self.rps_float), min_rps_f)

        # Applied RPS = clamped float value.
        # Round a bit for stability in logs/DB.
        self.current_rps = float(round(float(self.rps_float), 4))

        # ---- Concurrency ----
        base_c = max(1, _as_int(self.base_concurrency, 1))
        min_c = max(1, _as_int(self.min_concurrency, 1))
        max_c = max(min_c, _as_int(self.max_concurrency, max(min_c, base_c)))

        self.base_concurrency = int(base_c)
        self.min_concurrency = int(min_c)
        self.max_concurrency = int(max_c)

        self.concurrency_float = _as_float(self.concurrency_float, float(base_c))
        self.concurrency_float = _clamp_float(self.concurrency_float, float(min_c), float(max_c))

        if self.locked_concurrency is not None:
            lc = _as_float(self.locked_concurrency, float(self.concurrency_float))
            lc = _clamp_float(lc, float(min_c), float(max_c))
            self.locked_concurrency = lc
            # Apply ceiling.
            self.concurrency_float = min(float(self.concurrency_float), float(lc))
            self.concurrency_float = max(float(self.concurrency_float), float(min_c))

        current_concurrency = int(math.floor(float(self.concurrency_float) + 0.5))
        current_concurrency = max(min_c, min(max_c, current_concurrency))
        self.current_concurrency = int(current_concurrency)

        # ---- timestamps ----
        if self.updated_at is None:
            self.updated_at = datetime.now(timezone.utc)
        elif self.updated_at.tzinfo is None:
            self.updated_at = self.updated_at.replace(tzinfo=timezone.utc)
        else:
            self.updated_at = self.updated_at.astimezone(timezone.utc)

        if self.cooldown_until is not None:
            if self.cooldown_until.tzinfo is None:
                self.cooldown_until = self.cooldown_until.replace(tzinfo=timezone.utc)
            else:
                self.cooldown_until = self.cooldown_until.astimezone(timezone.utc)

    def to_mongo(self) -> Dict[str, Any]:
        return {
            "user_id": self.user_id,
            "endpoint_key": self.endpoint_key,

            # RPS
            "base_rps": float(self.base_rps),
            "min_rps": float(self.min_rps),
            "max_rps": float(self.max_rps),
            "rps_float": float(self.rps_float),
            "current_rps": float(self.current_rps),
            "locked_rps": float(self.locked_rps) if self.locked_rps is not None else None,

            # Concurrency
            "base_concurrency": int(self.base_concurrency),
            "min_concurrency": int(self.min_concurrency),
            "max_concurrency": int(self.max_concurrency),
            "concurrency_float": float(self.concurrency_float),
            "current_concurrency": int(self.current_concurrency),
            "locked_concurrency": (
                float(self.locked_concurrency) if self.locked_concurrency is not None else None
            ),

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

            # RPS
            base_rps=_as_float(doc.get("base_rps"), float(cfg.base_rps)),
            min_rps=_as_float(doc.get("min_rps"), float(cfg.min_rps)),
            max_rps=_as_float(doc.get("max_rps"), float(cfg.max_rps)),

            # Concurrency
            base_concurrency=_as_int(doc.get("base_concurrency"), int(cfg.base_concurrency)),
            min_concurrency=_as_int(doc.get("min_concurrency"), int(cfg.min_concurrency)),
            max_concurrency=_as_int(doc.get("max_concurrency"), int(cfg.max_concurrency)),

            # Learned
            rps_float=_as_float(doc.get("rps_float"), float(cfg.base_rps)),
            concurrency_float=_as_float(doc.get("concurrency_float"), float(cfg.base_concurrency)),
            current_rps=_as_float(doc.get("current_rps"), float(cfg.base_rps)),
            current_concurrency=_as_int(doc.get("current_concurrency"), int(cfg.base_concurrency)),

            locked_rps=(
                _as_float(doc.get("locked_rps"), float(cfg.base_rps))
                if doc.get("locked_rps") is not None
                else None
            ),
            locked_concurrency=(
                _as_float(doc.get("locked_concurrency"), float(cfg.base_concurrency))
                if doc.get("locked_concurrency") is not None
                else None
            ),

            cooldown_until=_as_dt(doc.get("cooldown_until")),
            consecutive_successes=_as_int(doc.get("consecutive_successes"), 0),
            consecutive_429s=_as_int(doc.get("consecutive_429s"), 0),
            success_count=_as_int(doc.get("success_count"), 0),
            error_count=_as_int(doc.get("error_count"), 0),
            last_status=(
                _as_int(doc.get("last_status"), 0) if doc.get("last_status") is not None else None
            ),
            updated_at=_as_dt(doc.get("updated_at")) or datetime.now(timezone.utc),
        )
        state.clamp_and_recompute()
        return state
