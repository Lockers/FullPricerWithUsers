from __future__ import annotations

import math
import re
from datetime import date
from typing import Optional, Tuple

from .curves import Curve, GRADE_MULTIPLIERS
from .types import Grade, Segment


_DAYS_PER_MONTH = 30.4375


def months_since(release_date: date, as_of: date) -> float:
    days = (as_of - release_date).days
    if days <= 0:
        return 0.0
    return days / _DAYS_PER_MONTH


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def retention_at(months: float, curve: Curve) -> float:
    m = curve.knots_months
    r = curve.knots_retention
    if len(m) != len(r) or len(m) < 2:
        raise ValueError("Invalid curve knots")

    if months <= m[0]:
        return clamp(r[0], curve.floor_ratio, 1.0)
    if months >= m[-1]:
        return clamp(r[-1], curve.floor_ratio, 1.0)

    for i in range(len(m) - 1):
        if m[i] <= months <= m[i + 1]:
            w = (months - m[i]) / (m[i + 1] - m[i])
            # log-linear interpolation (exponential segment)
            log_r0 = math.log(max(r[i], 1e-12))
            log_r1 = math.log(max(r[i + 1], 1e-12))
            log_rt = (1 - w) * log_r0 + w * log_r1
            return clamp(math.exp(log_rt), curve.floor_ratio, 1.0)

    return curve.floor_ratio


def round_gbp(x: float) -> float:
    # Round to nearest £5
    return float(round(x / 5.0) * 5.0)


def normalize_grade(raw: Optional[str], default: Grade = Grade.GOOD) -> Grade:
    if raw is None:
        return default
    s = str(raw).strip().upper()

    if "EXCELLENT" in s or s in {"A", "LIKE_NEW", "LIKENEW"} or "FUNCTIONAL_EXCELLENT" in s:
        return Grade.EXCELLENT
    if "GOOD" in s or s in {"B"} or "FUNCTIONAL_GOOD" in s:
        return Grade.GOOD
    if "FAIR" in s or "USED" in s or s in {"C"} or "FUNCTIONAL_FAIR" in s:
        return Grade.FAIR

    return default


def derive_segment(brand: str, model: str) -> Segment:
    b = (brand or "").strip().upper()
    m = (model or "").strip().upper()

    if b in {"IPHONE", "APPLE"}:
        if "PRO" in m:
            return Segment.APPLE_PRO
        return Segment.APPLE_BASE

    if b in {"SAMSUNG"}:
        if any(x in m for x in ["S", "NOTE", "ULTRA", "FOLD", "FLIP"]):
            return Segment.SAMSUNG_FLAGSHIP
        return Segment.GENERIC_ANDROID

    if b in {"GOOGLE", "PIXEL"} or "PIXEL" in m:
        return Segment.GOOGLE_PIXEL

    return Segment.GENERIC_ANDROID


def grade_multiplier(grade: Grade) -> float:
    return float(GRADE_MULTIPLIERS[grade])


def parse_storage_gb(token: str) -> Optional[int]:
    if token is None:
        return None
    t = token.strip().upper()
    m = re.search(r"(\d+)\s*GB", t)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def parse_sku(sku: str) -> Tuple[str, str, int, Grade]:
    parts = [p.strip() for p in str(sku).split("-") if p.strip()]
    if len(parts) < 4:
        raise ValueError(f"Unparseable sku: {sku}")

    brand = parts[0].upper()

    idx_storage = None
    storage_gb = None
    for i, p in enumerate(parts):
        sg = parse_storage_gb(p)
        if sg is not None:
            idx_storage = i
            storage_gb = sg
            break
    if idx_storage is None or storage_gb is None:
        raise ValueError(f"Could not find storage in sku: {sku}")

    grade = normalize_grade(parts[-1])

    # Heuristic:
    # - listing_sku: BRAND-MODEL-COLOUR-STORAGE-SIM-GRADE
    # - trade_sku: BRAND-MODEL-STORAGE-GRADE
    if len(parts) >= 6 and idx_storage >= 3:
        model = parts[1].upper()
    else:
        model_tokens = parts[1:idx_storage]
        model = "-".join(model_tokens).upper()

    return brand, model, storage_gb, grade


_SPACE_RE = re.compile(r"\s+")
_STORAGE_RE = re.compile(r"\b(\d{2,4})\s*GB\b", re.I)

def canonicalize_device(brand: str, model: str, storage_gb: int) -> Tuple[str, str, int]:
    b = (brand or "").strip().upper()
    m = (model or "").strip().upper()
    m = _SPACE_RE.sub(" ", m).strip()

    # Apple / iPhone normalization
    if b in {"IPHONE", "APPLE"}:
        b = "APPLE"
        if not m.startswith("IPHONE"):
            m = f"IPHONE {m}".strip()

    # Google / Pixel normalization
    if b in {"PIXEL", "GOOGLE"}:
        b = "GOOGLE"
        if "PIXEL" not in m:
            m = f"PIXEL {m}".strip()

    return b, m, int(storage_gb)


def parse_from_product(brand_hint: str, product: str) -> Tuple[str, str, int]:
    p = str(product or "").strip()
    if not p:
        raise ValueError("missing product")

    m = _STORAGE_RE.search(p)
    if not m:
        raise ValueError("no storage token in product")
    storage_gb = int(m.group(1))

    before_storage = p[: m.start()].strip()  # e.g. "iPhone 15 Plus"
    model = _SPACE_RE.sub(" ", before_storage).strip().upper()

    brand = str(brand_hint or "").strip().upper()

    # If brand hint is missing, infer minimally
    if not brand:
        if "IPHONE" in model or model.startswith("IPHONE"):
            brand = "APPLE"
        elif "PIXEL" in model:
            brand = "GOOGLE"

    return canonicalize_device(brand, model, storage_gb)

