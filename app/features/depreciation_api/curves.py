from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from .types import Grade, Segment


@dataclass(frozen=True)
class Curve:
    knots_months: List[float]
    knots_retention: List[float]
    floor_ratio: float = 0.05


# Baseline curves represent *EXCELLENT* condition.
# You will calibrate the level using multipliers from real sales data.
#
# These are intentionally conservative, smooth, and monotonic; treat them as seeds.
CURVE_VERSION = "seed_v1_excellent"

SEED_CURVES_EXCELLENT: Dict[Segment, Curve] = {
    Segment.APPLE_PRO: Curve(
        knots_months=[0, 3, 6, 12, 24, 36, 48, 60, 72, 84],
        knots_retention=[1.00, 0.80, 0.73, 0.62, 0.50, 0.40, 0.32, 0.25, 0.20, 0.16],
        floor_ratio=0.08,
    ),
    Segment.APPLE_BASE: Curve(
        knots_months=[0, 3, 6, 12, 24, 36, 48, 60, 72, 84],
        knots_retention=[1.00, 0.78, 0.70, 0.60, 0.46, 0.34, 0.26, 0.20, 0.16, 0.13],
        floor_ratio=0.07,
    ),
    Segment.SAMSUNG_FLAGSHIP: Curve(
        knots_months=[0, 3, 6, 12, 24, 36, 48, 60, 72, 84],
        knots_retention=[1.00, 0.75, 0.62, 0.45, 0.32, 0.23, 0.16, 0.12, 0.09, 0.07],
        floor_ratio=0.06,
    ),
    Segment.GOOGLE_PIXEL: Curve(
        knots_months=[0, 3, 6, 12, 24, 36, 48, 60, 72, 84],
        knots_retention=[1.00, 0.70, 0.55, 0.40, 0.25, 0.17, 0.11, 0.08, 0.06, 0.045],
        floor_ratio=0.05,
    ),
    Segment.GENERIC_ANDROID: Curve(
        knots_months=[0, 3, 6, 12, 24, 36, 48, 60, 72, 84],
        knots_retention=[1.00, 0.72, 0.58, 0.42, 0.28, 0.20, 0.14, 0.10, 0.08, 0.06],
        floor_ratio=0.05,
    ),
}

# Condition multipliers relative to EXCELLENT.
# Your stated business priors:
#   GOOD ~10% below EXCELLENT
#   FAIR ~25% below EXCELLENT
GRADE_MULTIPLIERS = {
    Grade.EXCELLENT: 1.00,
    Grade.GOOD: 0.90,
    Grade.FAIR: 0.75,
}
