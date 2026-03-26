from __future__ import annotations
from dataclasses import dataclass
from datetime import date
from models.instrument import Instrument, InstrumentCategory


@dataclass
class EligibilityResult:
    eligible: bool
    reason: str
    mrel_layer: str  # "subordination", "total", "excluded"
    crr2_article: str | None = None


def assess_mrel_eligibility(inst: Instrument, ref_date: date) -> EligibilityResult:
    """Assess MREL eligibility per CRR2 Art. 72a and BRRD2 Art. 45b."""
    if inst.category == InstrumentCategory.COVERED_BOND:
        return EligibilityResult(
            eligible=False,
            reason="Covered bonds excluded from MREL",
            mrel_layer="excluded",
            crr2_article="Art. 72a(2)(d)",
        )

    if inst.category == InstrumentCategory.CERTIFICATE:
        return EligibilityResult(
            eligible=False,
            reason="Non-protected certificate: principal at risk, excluded from MREL",
            mrel_layer="excluded",
            crr2_article="Art. 72a(2)(l)",
        )

    if not inst.is_maturity_eligible(ref_date):
        return EligibilityResult(
            eligible=False,
            reason=f"Residual maturity < 1 year (maturity: {inst.maturity_date})",
            mrel_layer="excluded",
            crr2_article="Art. 72c(1)",
        )

    subordination_categories = {
        InstrumentCategory.CET1,
        InstrumentCategory.AT1,
        InstrumentCategory.TIER2,
        InstrumentCategory.SENIOR_NON_PREFERRED,
    }

    if inst.category in subordination_categories:
        return EligibilityResult(
            eligible=True,
            reason=f"{inst.category.value}: counts towards subordination and total MREL",
            mrel_layer="subordination",
            crr2_article="Art. 72a(1)",
        )

    if inst.category in (
        InstrumentCategory.SENIOR_PREFERRED,
        InstrumentCategory.STRUCTURED_NOTE_PROTECTED,
    ):
        return EligibilityResult(
            eligible=True,
            reason=f"{inst.category.value}: counts towards total MREL only",
            mrel_layer="total",
            crr2_article="Art. 72a(1)",
        )

    return EligibilityResult(
        eligible=False,
        reason=f"Unknown category: {inst.category.value}",
        mrel_layer="excluded",
    )
