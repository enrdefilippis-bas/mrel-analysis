from __future__ import annotations

import pytest

from dashboard.pdf_pillar3_supplement import (
    build_km2_records,
    build_unicredit_december_2025_records,
)


def test_build_km2_records_parses_english_snippet() -> None:
    text = (
        "Template EU KM2 - Key metrics (€ million) "
        "1 Own Funds and eligible liabilities 92,497 "
        "EU-1a of which Own Funds and subordinated liabilities 69,045 "
        "2 Total risk exposure amount of the resolution group (TREA) 287,743 "
        "3 Own Funds and eligible liabilities as a percentage of TREA 32.15% "
        "EU-3a of which Own Funds and subordinated liabilities 24.00% "
        "4 Total exposure measure (TEM) of the resolution group 896,716 "
        "5 Own Funds and eligible liabilities as percentage of the TEM 10.32% "
        "EU-5a of which Own Funds or subordinated liabilities 7.70% "
        "EU-7 MREL expressed as percentage of the TREA 27.02% "
        "EU-8 of which to be met with Own Funds or subordinated liabilities 19.33% "
        "EU-9 MREL expressed as percentage of the TEM 5.98% "
        "EU-10 of which to be met with Own Funds or subordinated liabilities 5.98%"
    )

    records = build_km2_records("UniCredit S.p.A.", text)
    by_row = {record["row"]: record for record in records}

    assert by_row["0010"]["fact_value_numeric"] == pytest.approx(92_497_000_000.0)
    assert by_row["0040"]["fact_value_numeric"] == pytest.approx(0.3215)
    assert by_row["0120"]["fact_value_numeric"] == pytest.approx(0.2702)


def test_build_km2_records_parses_italian_snippet() -> None:
    text = (
        "EU KM2: metriche principali "
        "1 Fondi Propri e passività ammissibili 19.086 "
        "EU-1a Di cui fondi propri e passività subordinate 13.209 "
        "2 Importo complessivo dell’esposizione al rischio (TREA) del gruppo soggetto a risoluzione 55.597 "
        "3 Fondi propri e passività ammissibili in percentuale del TREA 34,33% "
        "EU-3a Di cui fondi propri e passività subordinate 23,76% "
        "4 Misura dell’esposizione complessiva (TEM) del gruppo soggetto a risoluzione 149.894 "
        "5 Fondi propri e passività ammissibili in percentuale della TEM 12,73% "
        "EU-5a Di cui fondi propri o passività subordinate 8,81% "
        "EU-7 MREL espresso in percentuale del TREA 25,46% "
        "EU-8 Di cui da soddisfare con fondi propri o passività subordinate 18,60% "
        "EU-9 MREL espresso in percentuale della TEM 6,52% "
        "EU-10 Di cui da soddisfare con fondi propri o passività subordinate 6,52%"
    )

    records = build_km2_records("BPER Banca S.p.A.", text)
    by_row = {record["row"]: record for record in records}

    assert by_row["0010"]["fact_value_numeric"] == pytest.approx(19_086_000_000.0)
    assert by_row["0060"]["fact_value_numeric"] == pytest.approx(149_894_000_000.0)
    assert by_row["0150"]["fact_value_numeric"] == pytest.approx(0.0652)


def test_build_unicredit_december_2025_records_includes_km2_tlac1_and_tlac3() -> None:
    records = build_unicredit_december_2025_records()

    km2_rows = {
        record["row"]: record["fact_value_numeric"]
        for record in records
        if record["template"].startswith("K_90.01")
    }
    tlac1_rows = {
        record["row"]: record["fact_value_numeric"]
        for record in records
        if record["template"].startswith("K_91.00")
    }
    tlac3_description_rows = [
        record["fact_value"]
        for record in records
        if record["template"].startswith("K_97.00") and record["row"] == "0010" and record["column"] == "0010"
    ]
    tlac3_total_rows = {
        record["row"]: record["fact_value_numeric"]
        for record in records
        if record["template"].startswith("K_97.00") and record["column"] == "0050"
    }

    assert km2_rows["0010"] == pytest.approx(90_655_000_000.0)
    assert km2_rows["0120"] == pytest.approx(0.2705)
    assert tlac1_rows["0250"] == pytest.approx(90_655_000_000.0)
    assert tlac1_rows["0330"] == pytest.approx(0.0833)
    assert tlac3_description_rows == [
        "EQUITY",
        "SUBORDINATED DEBTS",
        "SENIOR UNPREFERRED DEBTS",
        "UNSECURED DEBTS",
    ]
    assert tlac3_total_rows["0050"] == pytest.approx(96_527_000_000.0)
