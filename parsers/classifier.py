from __future__ import annotations
from datetime import date
import re
from parsers.prospectus import ProspectusData
from models.instrument import Instrument, InstrumentCategory, CouponType


def classify_instrument(data: ProspectusData) -> InstrumentCategory:
    """Classify an instrument based on parsed prospectus data.

    Priority order (most specific first):
    1. Covered Bond
    2. Tier 2 (subordinated)
    3. Senior Non-Preferred (Art. 12c CRR2)
    4. Certificate (non-protected: underlying-linked + barrier/autocallable, no capital protection)
    5. Structured Note (capital-protected: underlying-linked + capital protection)
    6. Senior Preferred (plain vanilla)
    7. Unknown
    """
    if data.is_covered_bond:
        return InstrumentCategory.COVERED_BOND

    if data.is_subordinated and not data.is_senior_non_preferred:
        return InstrumentCategory.TIER2

    if data.is_senior_non_preferred:
        return InstrumentCategory.SENIOR_NON_PREFERRED

    if data.is_underlying_linked:
        if data.has_barrier or data.has_autocallable:
            if data.is_capital_protected:
                return InstrumentCategory.STRUCTURED_NOTE_PROTECTED
            return InstrumentCategory.CERTIFICATE
        if data.is_capital_protected:
            return InstrumentCategory.STRUCTURED_NOTE_PROTECTED
        return InstrumentCategory.CERTIFICATE

    if not data.is_subordinated:
        return InstrumentCategory.SENIOR_PREFERRED

    return InstrumentCategory.UNKNOWN


def _parse_date(date_str: str | None) -> date | None:
    if not date_str:
        return None
    m = re.match(r"(\d{1,2})[/.\-](\d{1,2})[/.\-](\d{4})", date_str.strip())
    if m:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    return None


def _map_coupon_type(data: ProspectusData) -> CouponType:
    if data.coupon_type:
        ct = data.coupon_type.lower()
        if "fisso" in ct or "fixed" in ct:
            return CouponType.FIXED
        if "variabile" in ct or "float" in ct:
            return CouponType.FLOATING
        if "zero" in ct:
            return CouponType.ZERO_COUPON
        if "step up" in ct:
            return CouponType.STEP_UP
        if "step down" in ct:
            return CouponType.STEP_DOWN
    if data.is_underlying_linked:
        return CouponType.STRUCTURED
    return CouponType.UNKNOWN


def prospectus_to_instrument(data: ProspectusData) -> Instrument:
    category = classify_instrument(data)
    rank_map = {
        InstrumentCategory.CET1: 1,
        InstrumentCategory.AT1: 2,
        InstrumentCategory.TIER2: 3,
        InstrumentCategory.SENIOR_NON_PREFERRED: 4,
        InstrumentCategory.SENIOR_PREFERRED: 5,
        InstrumentCategory.STRUCTURED_NOTE_PROTECTED: 5,
        InstrumentCategory.CERTIFICATE: 5,
        InstrumentCategory.COVERED_BOND: None,
    }

    return Instrument(
        isin=data.isin or "UNKNOWN",
        name=data.instrument_name or "Unknown Instrument",
        category=category,
        issue_date=_parse_date(data.issue_date),
        maturity_date=_parse_date(data.maturity_date),
        coupon_type=_map_coupon_type(data),
        outstanding_amount=None,
        currency=data.currency or "EUR",
        crr2_rank=rank_map.get(category),
        mrel_eligible=None,
        bail_in_clause=data.has_bail_in_clause,
        capital_protected=data.is_capital_protected,
        capital_protection_pct=data.capital_protection_pct,
        original_amount=data.original_amount,
        underlying_linked=data.is_underlying_linked,
        classification_confidence=data.confidence,
        raw_prospectus_text=None,
    )
