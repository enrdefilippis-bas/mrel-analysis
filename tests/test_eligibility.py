from datetime import date
from models.instrument import Instrument, InstrumentCategory, CouponType
from models.eligibility import assess_mrel_eligibility
from models.mrel_stack import MRELStack

REF_DATE = date(2024, 12, 31)

def _make_instrument(category, maturity_date, amount=500_000_000):
    return Instrument(
        isin="IT0001234567", name="Test", category=category,
        issue_date=date(2024, 1, 1), maturity_date=maturity_date,
        coupon_type=CouponType.FIXED, outstanding_amount=amount,
        currency="EUR", crr2_rank=5,
    )

def test_senior_preferred_eligible():
    inst = _make_instrument(InstrumentCategory.SENIOR_PREFERRED, date(2027, 1, 1))
    result = assess_mrel_eligibility(inst, REF_DATE)
    assert result.eligible is True
    assert result.mrel_layer == "total"

def test_certificate_excluded():
    inst = _make_instrument(InstrumentCategory.CERTIFICATE, date(2027, 1, 1))
    result = assess_mrel_eligibility(inst, REF_DATE)
    assert result.eligible is False
    assert result.mrel_layer == "excluded"

def test_maturity_under_1yr_excluded():
    inst = _make_instrument(InstrumentCategory.SENIOR_PREFERRED, date(2025, 6, 1))
    result = assess_mrel_eligibility(inst, REF_DATE)
    assert result.eligible is False

def test_snp_subordination():
    inst = _make_instrument(InstrumentCategory.SENIOR_NON_PREFERRED, date(2028, 1, 1))
    result = assess_mrel_eligibility(inst, REF_DATE)
    assert result.eligible is True
    assert result.mrel_layer == "subordination"

def test_structured_note_protected_eligible():
    inst = _make_instrument(InstrumentCategory.STRUCTURED_NOTE_PROTECTED, date(2027, 1, 1))
    result = assess_mrel_eligibility(inst, REF_DATE)
    assert result.eligible is True
    assert result.mrel_layer == "total"

def test_mrel_stack_computation():
    instruments = [
        _make_instrument(InstrumentCategory.TIER2, date(2030, 1, 1), 200_000_000),
        _make_instrument(InstrumentCategory.SENIOR_NON_PREFERRED, date(2029, 1, 1), 300_000_000),
        _make_instrument(InstrumentCategory.SENIOR_PREFERRED, date(2028, 1, 1), 400_000_000),
        _make_instrument(InstrumentCategory.CERTIFICATE, date(2027, 1, 1), 100_000_000),
    ]
    stack = MRELStack.from_instruments(instruments, REF_DATE)
    assert stack.subordination_capacity == 500_000_000
    assert stack.total_mrel_capacity == 900_000_000
    assert stack.excluded_certificates == 100_000_000
