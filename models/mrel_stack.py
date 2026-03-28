from __future__ import annotations
import math
from dataclasses import dataclass
from datetime import date
from models.instrument import Instrument, InstrumentCategory
from models.eligibility import assess_mrel_eligibility


@dataclass
class MRELStack:
    ref_date: date
    cet1: float = 0.0
    at1: float = 0.0
    tier2: float = 0.0
    senior_non_preferred: float = 0.0
    senior_preferred: float = 0.0
    structured_notes_protected: float = 0.0
    excluded_certificates: float = 0.0
    excluded_covered_bonds: float = 0.0
    excluded_maturity: float = 0.0
    excluded_other: float = 0.0

    @property
    def subordination_capacity(self) -> float:
        return self.cet1 + self.at1 + self.tier2 + self.senior_non_preferred

    @property
    def total_mrel_capacity(self) -> float:
        return (
            self.subordination_capacity
            + self.senior_preferred
            + self.structured_notes_protected
        )

    @property
    def total_excluded(self) -> float:
        return (
            self.excluded_certificates
            + self.excluded_covered_bonds
            + self.excluded_maturity
            + self.excluded_other
        )

    @classmethod
    def from_instruments(
        cls,
        instruments: list[Instrument],
        ref_date: date,
        pillar3_overrides: dict[str, float] | None = None,
    ) -> MRELStack:
        stack = cls(ref_date=ref_date)
        category_map = {
            InstrumentCategory.CET1: "cet1",
            InstrumentCategory.AT1: "at1",
            InstrumentCategory.TIER2: "tier2",
            InstrumentCategory.SENIOR_NON_PREFERRED: "senior_non_preferred",
            InstrumentCategory.SENIOR_PREFERRED: "senior_preferred",
            InstrumentCategory.STRUCTURED_NOTE_PROTECTED: "structured_notes_protected",
        }

        # Categories overridden by Pillar 3 — skip bottom-up for these
        overridden_attrs = set()
        if pillar3_overrides:
            for attr, val in pillar3_overrides.items():
                setattr(stack, attr, val)
                overridden_attrs.add(attr)

        for inst in instruments:
            raw = inst.outstanding_amount
            amount = 0.0 if raw is None or (isinstance(raw, float) and math.isnan(raw)) else raw
            result = assess_mrel_eligibility(inst, ref_date)

            # DB override: if mrel_eligible is explicitly False, treat as ineligible
            eligible = result.eligible if inst.mrel_eligible is None else bool(inst.mrel_eligible)

            if eligible:
                attr = category_map.get(inst.category)
                if attr and attr not in overridden_attrs:
                    setattr(stack, attr, getattr(stack, attr) + amount)
            else:
                if inst.category == InstrumentCategory.CERTIFICATE:
                    stack.excluded_certificates += amount
                elif inst.category == InstrumentCategory.COVERED_BOND:
                    stack.excluded_covered_bonds += amount
                elif not inst.is_maturity_eligible(ref_date):
                    stack.excluded_maturity += amount
                else:
                    stack.excluded_other += amount

        return stack

    def to_dict(self) -> dict:
        return {
            "CET1": self.cet1,
            "AT1": self.at1,
            "Tier 2": self.tier2,
            "Senior Non-Preferred": self.senior_non_preferred,
            "Senior Preferred": self.senior_preferred,
            "Structured Notes (Protected)": self.structured_notes_protected,
            "Total Subordination": self.subordination_capacity,
            "Total MREL": self.total_mrel_capacity,
            "Excluded - Certificates": self.excluded_certificates,
            "Excluded - Covered Bonds": self.excluded_covered_bonds,
            "Excluded - Maturity < 1yr": self.excluded_maturity,
            "Excluded - Other": self.excluded_other,
        }
