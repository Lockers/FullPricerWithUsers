# app/features/pricing_groups/constants.py
"""
Constants used by pricing group building.

These are not "BackMarket transport" details.
They are domain rules used to decide which trade-in listings are eligible
and how we map trade-in grades to sell-side conditions.
"""

from __future__ import annotations

# Allowed trade-in "functional" grades for grouping.
# These values come from BackMarket buyback listings (`aestheticGradeCode`).
ALLOWED_TRADEIN_GRADE_CODES: set[str] = {
    "FUNCTIONAL_FLAWLESS",
    "FUNCTIONAL_GOOD",
    "FUNCTIONAL_USED",
    "FUNCTIONAL_CRACKED",
}

# Sell-side condition labels found in sell SKUs.
SELL_CONDITION_EXCELLENT = "EXCELLENT"
SELL_CONDITION_GOOD = "GOOD"
SELL_CONDITION_FAIR = "FAIR"

DEFAULT_SELL_CONDITION = SELL_CONDITION_EXCELLENT
