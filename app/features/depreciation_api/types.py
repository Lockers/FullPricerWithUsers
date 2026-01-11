from __future__ import annotations

from enum import Enum


class Segment(str, Enum):
    # Segment determines which seed curve is used.
    APPLE_PRO = "APPLE_PRO"
    APPLE_BASE = "APPLE_BASE"
    SAMSUNG_FLAGSHIP = "SAMSUNG_FLAGSHIP"
    GOOGLE_PIXEL = "GOOGLE_PIXEL"
    GENERIC_ANDROID = "GENERIC_ANDROID"


class Grade(str, Enum):
    EXCELLENT = "EXCELLENT"
    GOOD = "GOOD"
    FAIR = "FAIR"


class MultiplierScope(str, Enum):
    SKU = "sku"
    MODEL = "model"
    SEGMENT = "segment"
    GLOBAL = "global"
