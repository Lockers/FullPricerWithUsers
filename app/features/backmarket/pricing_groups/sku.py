# app/features/pricing_groups/sku.py
"""
SKU parsing + normalization utilities.

We have two SKU formats:

Trade SKU (trade-in listing sku):
  MAKE-MODEL-STORAGE-CONDITION
  Example: SAMSUNG-GALAXY S23 ULTRA-256GB-FAIR
  (Some systems also produce ...-CRACKED)

Sell SKU (sell listing sku):
  MAKE-MODEL-COLOUR-STORAGE-SIMTYPE-CONDITION
  Example: SAMSUNG-GALAXY S23 ULTRA-ORANGE-256GB-SS-EXCELLENT

We use trade SKU as the "parent" identifier and attach matching sell listings
as children based on:
  - same MAKE / MODEL / STORAGE
  - sell CONDITION = target sell condition derived from trade-in grade
    (and CRACKED -> EXCELLENT rule)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from app.features.backmarket.pricing_groups.constants import (
    DEFAULT_SELL_CONDITION,
    SELL_CONDITION_EXCELLENT,
    SELL_CONDITION_FAIR,
    SELL_CONDITION_GOOD,
)

# -----------------------------------------------------------------------------
# Strict SKU rules (as per your requirement)
# -----------------------------------------------------------------------------
# Sell SKU must be:
#   MAKE-MODEL-COLOUR-STORAGE-SIMTYPE-CONDITION
# Where:
#   STORAGE: 128GB, 256GB, 512GB, 1000GB, 1TB, 2TB  (we normalize TB -> GB)
#   SIMTYPE: SS, ES, DS
#   CONDITION: FAIR, GOOD, EXCELLENT
#
# Trade SKU must be:
#   MAKE-MODEL-STORAGE-CONDITION
# Where:
#   STORAGE: same normalization rules as sell
#   CONDITION: FAIR, GOOD, EXCELLENT, CRACKED
# -----------------------------------------------------------------------------

ALLOWED_STORAGE_GB_VALUES: set[int] = {128, 256, 512, 1000, 2000}
ALLOWED_SELL_SIM_TYPES: set[str] = {"SS", "ES", "DS"}
ALLOWED_SELL_CONDITIONS: set[str] = {"FAIR", "GOOD", "EXCELLENT"}
ALLOWED_TRADE_CONDITIONS: set[str] = {"FAIR", "GOOD", "EXCELLENT", "CRACKED"}


def _upper(s: Optional[str]) -> str:
    return (s or "").strip().upper()


def _parse_storage_gb(storage_str: str) -> Optional[int]:
    """
    Convert storage segment to GB integer.

    Accepted (strict):
      - 128GB, 256GB, 512GB, 1000GB
      - 1TB, 2TB
      - 1024GB is normalized to 1000GB
      - 2048GB is normalized to 2000GB

    Returns:
      int GB value if valid and in ALLOWED_STORAGE_GB_VALUES,
      else None.
    """
    s = (storage_str or "").strip().upper().replace(" ", "")
    if not s:
        return None

    # GB form
    if s.endswith("GB"):
        num = s[:-2].strip()
        if not num.isdigit():
            return None
        n = int(num)

        # Normalize common "computer storage" variants to your canonical SKUs.
        if n == 1024:
            n = 1000
        elif n == 2048:
            n = 2000

        return n if n in ALLOWED_STORAGE_GB_VALUES else None

    # TB form
    if s.endswith("TB"):
        num = s[:-2].strip()
        if not num.isdigit():
            return None
        n_tb = int(num)
        if n_tb <= 0:
            return None
        n_gb = n_tb * 1000
        return n_gb if n_gb in ALLOWED_STORAGE_GB_VALUES else None

    return None


@dataclass(frozen=True)
class ParsedSellSku:
    brand: str
    model: str
    storage_str: str
    storage_gb: Optional[int]
    colour: str
    sim_type: str
    condition: str

    # This is the "equivalent" trade SKU:
    # MAKE-MODEL-STORAGE-CONDITION (no colour/sim_type)
    trade_sku: str


@dataclass(frozen=True)
class ParsedTradeSku:
    brand: str
    model: str
    storage_str: str
    storage_gb: Optional[int]
    condition: str


def parse_sell_sku_with_reason(
    sku: Optional[str],
) -> Tuple[Optional[ParsedSellSku], Optional[str], Dict[str, Any]]:
    """
    Strict sell SKU parse with diagnostics.

    Returns:
      (parsed, reason_code, reason_details)

    - If parsed is not None => success, reason_code is None.
    - If parsed is None => failure, reason_code explains why.
    """
    if not sku or not str(sku).strip():
        return None, "missing_sku", {}

    raw = str(sku)
    parts = raw.split("-")
    if len(parts) < 6:
        return (
            None,
            "sell_sku_bad_parts",
            {
                "sku": raw,
                "parts_count": len(parts),
                "expected_format": "MAKE-MODEL-COLOUR-STORAGE-SIMTYPE-CONDITION",
                "example": "SAMSUNG-GALAXY S23 ULTRA-ORANGE-256GB-SS-EXCELLENT",
            },
        )

    brand = (parts[0] or "").strip()
    condition = _upper(parts[-1])
    sim_type = _upper(parts[-2])
    storage_str = (parts[-3] or "").strip().upper().replace(" ", "")
    colour = (parts[-4] or "").strip()
    model_parts = parts[1:-4]
    model = "-".join(model_parts).strip() if model_parts else ""

    if not brand or not model or not colour:
        return (
            None,
            "sell_sku_missing_components",
            {"sku": raw, "brand": brand, "model": model, "colour": colour},
        )

    storage_gb = _parse_storage_gb(storage_str)
    if storage_gb is None:
        return (
            None,
            "sell_sku_invalid_storage",
            {
                "sku": raw,
                "storage": storage_str,
                "allowed_storage_gb_values": sorted(ALLOWED_STORAGE_GB_VALUES),
                "examples": ["256GB", "1000GB", "1TB", "2TB"],
            },
        )

    if sim_type not in ALLOWED_SELL_SIM_TYPES:
        return (
            None,
            "sell_sku_invalid_sim_type",
            {
                "sku": raw,
                "sim_type": sim_type,
                "allowed": sorted(ALLOWED_SELL_SIM_TYPES),
            },
        )

    if condition not in ALLOWED_SELL_CONDITIONS:
        return (
            None,
            "sell_sku_invalid_condition",
            {
                "sku": raw,
                "condition": condition,
                "allowed": sorted(ALLOWED_SELL_CONDITIONS),
            },
        )

    trade_sku = f"{brand}-{model}-{storage_str}-{condition}"

    return (
        ParsedSellSku(
            brand=brand,
            model=model,
            storage_str=storage_str,
            storage_gb=storage_gb,
            colour=colour,
            sim_type=sim_type,
            condition=condition,
            trade_sku=trade_sku,
        ),
        None,
        {},
    )


def parse_sell_sku(sku: Optional[str]) -> Optional[ParsedSellSku]:
    """
    Parse sell SKU:
      MAKE-MODEL-COLOUR-STORAGE-SIMTYPE-CONDITION

    Returns None if malformed.
    (Strict rules are enforced inside parse_sell_sku_with_reason.)
    """
    parsed, _, _ = parse_sell_sku_with_reason(sku)
    return parsed


def parse_trade_sku_with_reason(
    trade_sku: Optional[str],
) -> Tuple[Optional[ParsedTradeSku], Optional[str], Dict[str, Any]]:
    """
    Strict trade SKU parse with diagnostics.

    Trade SKU:
      MAKE-MODEL-STORAGE-CONDITION
    """
    if not trade_sku or not str(trade_sku).strip():
        return None, "missing_sku", {}

    raw = str(trade_sku)
    parts = raw.split("-")
    if len(parts) < 4:
        return (
            None,
            "trade_sku_bad_parts",
            {
                "sku": raw,
                "parts_count": len(parts),
                "expected_format": "MAKE-MODEL-STORAGE-CONDITION",
                "example": "SAMSUNG-GALAXY S23 ULTRA-256GB-FAIR",
            },
        )

    brand = (parts[0] or "").strip()
    condition = _upper(parts[-1])
    storage_str = (parts[-2] or "").strip().upper().replace(" ", "")
    model_parts = parts[1:-2]
    model = "-".join(model_parts).strip() if model_parts else ""

    if not brand or not model:
        return (
            None,
            "trade_sku_missing_components",
            {"sku": raw, "brand": brand, "model": model},
        )

    storage_gb = _parse_storage_gb(storage_str)
    if storage_gb is None:
        return (
            None,
            "trade_sku_invalid_storage",
            {
                "sku": raw,
                "storage": storage_str,
                "allowed_storage_gb_values": sorted(ALLOWED_STORAGE_GB_VALUES),
                "examples": ["256GB", "1000GB", "1TB", "2TB"],
            },
        )

    if condition not in ALLOWED_TRADE_CONDITIONS:
        return (
            None,
            "trade_sku_invalid_condition",
            {
                "sku": raw,
                "condition": condition,
                "allowed": sorted(ALLOWED_TRADE_CONDITIONS),
            },
        )

    return (
        ParsedTradeSku(
            brand=brand,
            model=model,
            storage_str=storage_str,
            storage_gb=storage_gb,
            condition=condition,
        ),
        None,
        {},
    )


def parse_trade_sku(trade_sku: Optional[str]) -> Optional[ParsedTradeSku]:
    """
    Parse trade SKU:
      MAKE-MODEL-STORAGE-CONDITION

    Returns None if malformed.
    (Strict rules are enforced inside parse_trade_sku_with_reason.)
    """
    parsed, _, _ = parse_trade_sku_with_reason(trade_sku)
    return parsed


def target_sell_condition(
    *,
    tradein_grade_code: Optional[str],
    trade_sku_condition: Optional[str],
) -> str:
    """
    Determine which sell-listing CONDITION we should attach as children.

    Priority:
    1) If tradein_grade_code is present, use mapping:
       FUNCTIONAL_FLAWLESS -> EXCELLENT
       FUNCTIONAL_GOOD     -> GOOD
       FUNCTIONAL_USED     -> FAIR
       FUNCTIONAL_CRACKED  -> EXCELLENT  (refurb cracked to top spec)
    2) Else, if trade SKU condition is CRACKED -> EXCELLENT
    3) Else, fall back to trade SKU condition (uppercased)
    4) Else, default EXCELLENT
    """
    g = _upper(tradein_grade_code)
    if g == "FUNCTIONAL_FLAWLESS":
        return SELL_CONDITION_EXCELLENT
    if g == "FUNCTIONAL_GOOD":
        return SELL_CONDITION_GOOD
    if g == "FUNCTIONAL_USED":
        return SELL_CONDITION_FAIR
    if g == "FUNCTIONAL_CRACKED":
        return SELL_CONDITION_EXCELLENT

    c = _upper(trade_sku_condition)
    if c == "CRACKED":
        return SELL_CONDITION_EXCELLENT
    if c:
        return c

    return DEFAULT_SELL_CONDITION


def make_group_key(
    *,
    brand: str,
    model: str,
    storage_gb: Optional[int],
    tradein_grade_code: Optional[str],
) -> str:
    """
    Stable key to identify a group inside a user.

    Example:
      SAMSUNG|GALAXY S23 ULTRA|256|FUNCTIONAL_FLAWLESS
    """
    sg = storage_gb if storage_gb is not None else 0
    g = _upper(tradein_grade_code) or "UNKNOWN"
    return f"{_upper(brand)}|{_upper(model)}|{sg}|{g}"
