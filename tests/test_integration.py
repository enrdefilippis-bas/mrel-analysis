"""End-to-end test with synthetic data — no network calls."""
import sqlite3
from datetime import date
from pathlib import Path
from models.instrument import Instrument, InstrumentCategory, CouponType
from models.eligibility import assess_mrel_eligibility
from models.mrel_stack import MRELStack
from pipeline import init_db, save_instrument, load_instruments

REF_DATE = date(2024, 12, 31)

SAMPLE_INSTRUMENTS = [
    Instrument(
        isin="IT0005000001", name="BBPM Senior 4.5% 2027",
        category=InstrumentCategory.SENIOR_PREFERRED,
        issue_date=date(2024, 1, 15), maturity_date=date(2027, 1, 15),
        coupon_type=CouponType.FIXED, outstanding_amount=500_000_000,
        currency="EUR", crr2_rank=5,
    ),
    Instrument(
        isin="XS0050000002", name="BBPM SNP 3.75% 2029",
        category=InstrumentCategory.SENIOR_NON_PREFERRED,
        issue_date=date(2023, 6, 1), maturity_date=date(2029, 6, 1),
        coupon_type=CouponType.FIXED, outstanding_amount=750_000_000,
        currency="EUR", crr2_rank=4,
    ),
    Instrument(
        isin="IT0005000003", name="BBPM Tier 2 5% 2034",
        category=InstrumentCategory.TIER2,
        issue_date=date(2024, 3, 1), maturity_date=date(2034, 3, 1),
        coupon_type=CouponType.FIXED, outstanding_amount=300_000_000,
        currency="EUR", crr2_rank=3,
    ),
    Instrument(
        isin="IT0005000004", name="BBPM Cert Autocall FTSE MIB",
        category=InstrumentCategory.CERTIFICATE,
        issue_date=date(2024, 2, 1), maturity_date=date(2027, 2, 1),
        coupon_type=CouponType.STRUCTURED, outstanding_amount=50_000_000,
        currency="EUR", crr2_rank=5,
    ),
    Instrument(
        isin="IT0005000005", name="BBPM Structured Capital Protected",
        category=InstrumentCategory.STRUCTURED_NOTE_PROTECTED,
        issue_date=date(2024, 4, 1), maturity_date=date(2028, 4, 1),
        coupon_type=CouponType.STRUCTURED, outstanding_amount=200_000_000,
        currency="EUR", crr2_rank=5,
    ),
    Instrument(
        isin="IT0005000006", name="BBPM Senior Expiring Soon",
        category=InstrumentCategory.SENIOR_PREFERRED,
        issue_date=date(2023, 1, 1), maturity_date=date(2025, 6, 1),
        coupon_type=CouponType.FIXED, outstanding_amount=100_000_000,
        currency="EUR", crr2_rank=5,
    ),
]


def test_full_pipeline_with_synthetic_data(tmp_path):
    db_path = tmp_path / "test.db"
    conn = init_db(db_path)

    for inst in SAMPLE_INSTRUMENTS:
        result = assess_mrel_eligibility(inst, REF_DATE)
        inst.mrel_eligible = result.eligible
        inst.eligibility_reason = result.reason
        save_instrument(conn, inst, result.mrel_layer)

    loaded = load_instruments(conn)
    assert len(loaded) == 6

    stack = MRELStack.from_instruments(loaded, REF_DATE)

    # Subordination: T2 (300M) + SNP (750M) = 1.05B
    assert stack.subordination_capacity == 1_050_000_000

    # Total MREL: sub (1.05B) + Senior 2027 (500M) + Structured Protected (200M) = 1.75B
    assert stack.total_mrel_capacity == 1_750_000_000

    # Excluded: certificate (50M) + maturity < 1yr senior (100M)
    assert stack.excluded_certificates == 50_000_000
    assert stack.excluded_maturity == 100_000_000

    conn.close()
