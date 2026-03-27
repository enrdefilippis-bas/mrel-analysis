from __future__ import annotations
from dataclasses import dataclass, field
from datetime import date
from enum import Enum


class InstrumentCategory(Enum):
    CET1 = "CET1"
    AT1 = "AT1"
    TIER2 = "Tier 2"
    SENIOR_NON_PREFERRED = "Senior Non-Preferred"
    SENIOR_PREFERRED = "Senior Preferred"
    STRUCTURED_NOTE_PROTECTED = "Structured Note (Capital Protected)"
    CERTIFICATE = "Certificate (Non-Protected)"
    COVERED_BOND = "Covered Bond"
    UNKNOWN = "Unknown"


class CouponType(Enum):
    FIXED = "Fixed"
    FLOATING = "Floating"
    ZERO_COUPON = "Zero Coupon"
    STEP_UP = "Step Up"
    STEP_DOWN = "Step Down"
    STRUCTURED = "Structured"
    UNKNOWN = "Unknown"


@dataclass
class Instrument:
    isin: str
    name: str
    category: InstrumentCategory
    issue_date: date | None
    maturity_date: date | None  # None = perpetual
    coupon_type: CouponType
    outstanding_amount: float | None
    currency: str
    crr2_rank: int | None = None
    coupon_rate: float | None = None
    listing_venue: str | None = None
    mrel_eligible: bool | None = None
    eligibility_reason: str | None = None
    prospectus_url: str | None = None
    source_pdf: str | None = None
    classification_confidence: float = 1.0
    bail_in_clause: bool | None = None
    capital_protected: bool | None = None
    capital_protection_pct: float | None = None  # e.g. 100.0 for 100% protection
    original_amount: float | None = None  # Original nominal amount issued (EUR)
    underlying_linked: bool | None = None
    raw_prospectus_text: str | None = None

    def residual_maturity_years(self, ref_date: date) -> float:
        if self.maturity_date is None:
            return float("inf")
        delta = self.maturity_date - ref_date
        return delta.days / 365.25

    def is_maturity_eligible(self, ref_date: date) -> bool:
        return self.residual_maturity_years(ref_date) > 1.0
