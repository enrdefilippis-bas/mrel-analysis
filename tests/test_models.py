from datetime import date
from models.instrument import Instrument, InstrumentCategory, CouponType

def test_instrument_creation():
    inst = Instrument(
        isin="IT0005692246",
        name="Banco BPM 4.5% 2027",
        category=InstrumentCategory.SENIOR_PREFERRED,
        issue_date=date(2024, 1, 15),
        maturity_date=date(2027, 1, 15),
        coupon_type=CouponType.FIXED,
        coupon_rate=4.5,
        outstanding_amount=500_000_000,
        currency="EUR",
        listing_venue="MOT",
        mrel_eligible=True,
        eligibility_reason="Plain vanilla senior, residual maturity > 1yr",
        crr2_rank=5,
    )
    assert inst.isin == "IT0005692246"
    assert inst.mrel_eligible is True

def test_residual_maturity():
    inst = Instrument(
        isin="IT0005692246", name="Test Bond",
        category=InstrumentCategory.SENIOR_PREFERRED,
        issue_date=date(2024, 1, 15), maturity_date=date(2027, 1, 15),
        coupon_type=CouponType.FIXED, outstanding_amount=500_000_000,
        currency="EUR", crr2_rank=5,
    )
    ref_date = date(2024, 12, 31)
    rm = inst.residual_maturity_years(ref_date)
    assert rm > 2.0
    assert rm < 2.1

def test_residual_maturity_expired():
    inst = Instrument(
        isin="IT0005692246", name="Expired Bond",
        category=InstrumentCategory.SENIOR_PREFERRED,
        issue_date=date(2020, 1, 1), maturity_date=date(2024, 6, 30),
        coupon_type=CouponType.FIXED, outstanding_amount=100_000_000,
        currency="EUR", crr2_rank=5,
    )
    rm = inst.residual_maturity_years(date(2024, 12, 31))
    assert rm < 0

def test_perpetual_residual_maturity():
    inst = Instrument(
        isin="XS1234567890", name="AT1 Perpetual",
        category=InstrumentCategory.AT1,
        issue_date=date(2020, 1, 1), maturity_date=None,
        coupon_type=CouponType.FIXED, outstanding_amount=500_000_000,
        currency="EUR", crr2_rank=2,
    )
    rm = inst.residual_maturity_years(date(2024, 12, 31))
    assert rm == float("inf")
