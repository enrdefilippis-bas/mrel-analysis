from __future__ import annotations

from pathlib import Path
import re

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent
CBR_TEXT_DIR = BASE_DIR / "cbr" / "text"

JUNE_2025 = "2025-06-30"
DECEMBER_2025 = "2025-12-31"
COUNTRY = "Italy"
KM2_TEMPLATE = (
    "K_90.01 - EU KM2 - Key metrics - MREL and, where applicable, "
    "G-SII requirement for own funds and eligible liabilities"
)
TLAC1_TEMPLATE = (
    "K_91.00 - EU TLAC1 - Composition - MREL and, where applicable, "
    "G-SII requirement for own funds and eligible liabilities"
)
TLAC3_TEMPLATE = "K_97.00 - EU TLAC3 - creditor ranking - resolution entity"

LONG_COLUMNS = [
    "entity_code",
    "entity_name",
    "country",
    "module_name",
    "module_code",
    "cell",
    "open_key",
    "template",
    "row",
    "row_name",
    "column",
    "column_name",
    "sheet",
    "reference_date",
    "fact_value",
]

PDF_TEXT_KM2_SOURCES = {
    ("UniCredit S.p.A.", JUNE_2025): CBR_TEXT_DIR / "unicredit-s-p-a.txt",
    ("Intesa Sanpaolo S.p.A.", JUNE_2025): CBR_TEXT_DIR / "intesa-sanpaolo-s-p-a.txt",
    ("BPER Banca S.p.A.", JUNE_2025): CBR_TEXT_DIR / "bper-banca-s-p-a.txt",
}

ROW_METADATA = {
    "0010": "1. Own funds and eligible liabilities",
    "0020": "EU-1a. Of which own funds and subordinated liabilities",
    "0030": "2. Total risk exposure amount of the resolution group TREA",
    "0040": "3. Own funds and eligible liabilities as a percentage of the TREA",
    "0050": "EU-3a. Of which own funds and subordinated liabilities",
    "0060": "4. Total exposure measure TEM of the resolution group",
    "0070": "5. Own funds and eligible liabilities as percentage of the TEM",
    "0080": "EU-5a. Of which own funds or subordinated liabilities",
    "0120": "EU-7. MREL expressed as a percentage of the TREA",
    "0130": "EU-8. Of which to be met with own funds or subordinated liabilities",
    "0140": "EU-9. MREL expressed as a percentage of the TEM",
    "0150": "EU-10. Of which to be met with own funds or subordinated liabilities",
}

UNICREDIT_DEC_2025_TLAC1_ROWS = {
    "0010": ("1. Common Equity Tier 1 capital CET1", 43_700_000_000.0),
    "0020": ("2. Additional Tier 1 capital AT1", 4_956_000_000.0),
    "0060": ("6. Tier 2 capital T2", 7_648_000_000.0),
    "0090": ("11. Own Funds for the purpose of articles 92a CRR and 45 BRRD arising from regulatory capital instruments (A)", 56_304_000_000.0),
    "0100": ("12. Eligible liabilities instruments issued directly by the resolution entity that are subordinated to excluded liabilities (not grandfathered) (B)", 11_221_000_000.0),
    "0110": ("EU12a Eligible liabilities instruments issued by other entities within the resolution group that are subordinated to excluded liabilities (not grandfathered)", 0.0),
    "0120": ("EU12b Eligible liabilities instruments that are subordinated to excluded liabilities, issued prior to 27 June 2019 - (subordinated grandfathered)", 0.0),
    "0130": ("EU12c Tier 2 instruments with a residual maturity of at least one year to the extent they do not qualify as Tier 2 items", 0.0),
    "0140": ("13. Eligible liabilities that are not subordinated to excluded liabilities (not grandfathered pre cap)", 24_275_000_000.0),
    "0150": ("EU13a Eligible liabilities that are not subordinated to excluded liabilities, issued prior to 27 June 2019 and grandfathered (pre-cap)", 482_000_000.0),
    "0160": ("14. Amount of non subordinated eligible liabilities instruments, where applicable after application of article 72b (3) CRR (C)", 24_757_000_000.0),
    "0190": ("17. Eligible liabilities items before adjustments", 35_978_000_000.0),
    "0200": ("EU17a of which subordinated liabilities items", 11_221_000_000.0),
    "0210": ("18. Own Funds and eligible liabilities items before adjustments", 92_283_000_000.0),
    "0220": ("19. (Deduction of exposures between MPE resolution groups)", 0.0),
    "0230": ("20. (Deduction of investments in other eligible liabilities instruments) (D)", 1_627_000_000.0),
    "0250": ("22. Own Funds and eligible liabilities after adjustments", 90_655_000_000.0),
    "0260": ("EU-22a Of which own funds and subordinated", 67_295_000_000.0),
    "0270": ("23. Total risk exposure amount (TREA)", 296_327_000_000.0),
    "0280": ("24. Total exposure measure (TEM)", 906_925_000_000.0),
    "0290": ("25. Own Funds and eligible liabilities as a percentage of TREA", 0.3059),
    "0300": ("EU-25a Of which own funds and subordinated", 0.2271),
    "0310": ("26. Own funds and eligible liabilities as a percentage of TEM", 0.10),
    "0320": ("EU-26a Of which own funds and subordinated", 0.0742),
    "0330": ("27. CET1 (as a percentage of TREA) available after meeting the resolution group’s requirements (E)", 0.0833),
}

UNICREDIT_DEC_2025_TLAC3_DESCRIPTIONS = (
    "EQUITY",
    "SUBORDINATED DEBTS",
    "SENIOR UNPREFERRED DEBTS",
    "UNSECURED DEBTS",
)

UNICREDIT_DEC_2025_TLAC3_VALUES = {
    "0020": ("2. Liabilities and own funds", [49_723_000_000.0, 11_595_000_000.0, 11_553_000_000.0, 92_896_000_000.0], 165_768_000_000.0),
    "0030": ("3. of which excluded liabilities", [0.0, 0.0, 0.0, 64_000_000.0], 64_000_000.0),
    "0040": ("4. Liabilities and own funds less excluded liabilities", [49_723_000_000.0, 11_595_000_000.0, 11_553_000_000.0, 92_832_000_000.0], 165_703_000_000.0),
    "0050": ("5. Subset of liabilities and own funds less excluded liabilities that are own funds and liabilities potentially eligible for meeting MREL/TLAC", [49_723_000_000.0, 11_307_000_000.0, 11_221_000_000.0, 24_275_000_000.0], 96_527_000_000.0),
}

FIELD_PATTERNS = {
    "0010": (
        re.compile(r"\b1\s+Own Funds and eligible liabilities\s+([0-9][0-9,\.]*)", re.IGNORECASE),
        re.compile(r"\b1\s+Fondi Propri e passività ammissibili\s+([0-9][0-9\.,]*)", re.IGNORECASE),
    ),
    "0020": (
        re.compile(r"EU-1a\s+of which Own Funds and subordinated liabilities\s+([0-9][0-9,\.]*)", re.IGNORECASE),
        re.compile(r"EU-1a\s+Di cui fondi propri e passività subordinate\s+([0-9][0-9\.,]*)", re.IGNORECASE),
    ),
    "0030": (
        re.compile(r"\b2\s+Total risk exposure amount of the resolution group \(TREA\)\s+([0-9][0-9,\.]*)", re.IGNORECASE),
        re.compile(r"\b2\s+Importo complessivo dell['’]esposizione al rischio \(TREA\) del gruppo soggetto a risoluzione\s+([0-9][0-9\.,]*)", re.IGNORECASE),
    ),
    "0040": (
        re.compile(r"\b3\s+Own Funds and eligible liabilities as a percentage of TREA\s+([0-9][0-9,\.]*)%", re.IGNORECASE),
        re.compile(r"\b3\s+Fondi Propri e passività ammissibili in percentuale del TREA\s+([0-9][0-9\.,]*)%", re.IGNORECASE),
    ),
    "0050": (
        re.compile(r"EU-3a\s+of which Own Funds and subordinated liabilities\s+([0-9][0-9,\.]*)%", re.IGNORECASE),
        re.compile(r"EU-3a\s+Di cui fondi propri e passività subordinate\s+([0-9][0-9\.,]*)%", re.IGNORECASE),
    ),
    "0060": (
        re.compile(r"\b4\s+Total exposure measure \(TEM\) of the resolution group\s+([0-9][0-9,\.]*)", re.IGNORECASE),
        re.compile(r"\b4\s+Misura dell['’]Esposizione complessiva \(TEM\) del gruppo soggetto a risoluzione\s+([0-9][0-9\.,]*)", re.IGNORECASE),
        re.compile(r"\b4\s+Misura dell['’]esposizione complessiva \(TEM\) del gruppo soggetto a risoluzione\s+([0-9][0-9\.,]*)", re.IGNORECASE),
    ),
    "0070": (
        re.compile(r"\b5\s+Own Funds and eligible liabilities as percentage of the TEM\s+([0-9][0-9,\.]*)%", re.IGNORECASE),
        re.compile(r"\b5\s+Fondi propri e passività ammissibili in percentuale della TEM\s+([0-9][0-9\.,]*)%", re.IGNORECASE),
    ),
    "0080": (
        re.compile(r"EU-5a\s+of which Own Funds or subordinated liabilities\s+([0-9][0-9,\.]*)%", re.IGNORECASE),
        re.compile(r"EU-5a\s+Di cui fondi propri o passività subordinate\s+([0-9][0-9\.,]*)%", re.IGNORECASE),
    ),
    "0120": (
        re.compile(r"EU-7\s+MREL expressed as percentage of the TREA\s+([0-9][0-9,\.]*)%", re.IGNORECASE),
        re.compile(r"EU-7\s+MREL espresso in percentuale del TREA\s+([0-9][0-9\.,]*)%", re.IGNORECASE),
    ),
    "0130": (
        re.compile(r"EU-8\s+of which to be met with Own Funds or subordinated liabilities\s+([0-9][0-9,\.]*)%", re.IGNORECASE),
        re.compile(r"EU-8\s+di cui da soddisfare con fondi propri o passività subordinate\s+([0-9][0-9\.,]*)%", re.IGNORECASE),
        re.compile(r"EU-8\s+Di cui da soddisfare con fondi propri o passività subordinate\s+([0-9][0-9\.,]*)%", re.IGNORECASE),
    ),
    "0140": (
        re.compile(r"EU-9\s+MREL expressed as percentage of the TEM\s+([0-9][0-9,\.]*)%", re.IGNORECASE),
        re.compile(r"EU-9\s+MREL espresso in percentuale della TEM\s+([0-9][0-9\.,]*)%", re.IGNORECASE),
    ),
    "0150": (
        re.compile(r"EU-10\s+of which to be met with Own Funds or subordinated liabilities\s+([0-9][0-9,\.]*)%", re.IGNORECASE),
        re.compile(r"EU-10\s+Di cui da soddisfare con fondi propri o passività subordinate\s+([0-9][0-9\.,]*)%", re.IGNORECASE),
    ),
}

AMOUNT_ROWS = {"0010", "0020", "0030", "0060"}


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=LONG_COLUMNS + ["fact_value_numeric"])


def _clean_page_markers(text: str) -> str:
    return re.sub(r"=== PAGE \d+ ===", " ", text)


def _parse_amount_token(token: str) -> float:
    digits = token.replace(".", "").replace(",", "").strip()
    return float(digits) * 1_000_000.0


def _parse_percentage_token(token: str) -> float:
    normalized = token.strip().replace(",", ".")
    return float(normalized) / 100.0


def _extract_row_value(text: str, row_code: str) -> float | None:
    for pattern in FIELD_PATTERNS[row_code]:
        match = pattern.search(text)
        if not match:
            continue
        token = match.group(1)
        if row_code in AMOUNT_ROWS:
            return _parse_amount_token(token)
        return _parse_percentage_token(token)
    return None


def _base_record(
    *,
    entity_name: str,
    reference_date: str,
    template: str,
    row: str,
    row_name: str,
    value: float | str,
    column: str = "0010",
    column_name: str = "a. T",
    module_code: str = "PDF_SUPPLEMENT",
) -> dict[str, object]:
    return {
        "entity_code": entity_name,
        "entity_name": entity_name,
        "country": COUNTRY,
        "module_name": "PDF Pillar 3 Supplement",
        "module_code": module_code,
        "cell": None,
        "open_key": None,
        "template": template,
        "row": row,
        "row_name": row_name,
        "column": column,
        "column_name": column_name,
        "sheet": "PDF supplement",
        "reference_date": reference_date,
        "fact_value": value,
        "fact_value_numeric": value if isinstance(value, (int, float)) else None,
    }


def build_km2_records(entity_name: str, text: str, reference_date: str = JUNE_2025) -> list[dict[str, object]]:
    cleaned_text = _clean_page_markers(text)
    records: list[dict[str, object]] = []
    for row_code, row_name in ROW_METADATA.items():
        value = _extract_row_value(cleaned_text, row_code)
        if value is None:
            continue
        records.append(
            _base_record(
                entity_name=entity_name,
                reference_date=reference_date,
                template=KM2_TEMPLATE,
                row=row_code,
                row_name=row_name,
                value=value,
                module_code="PDF_KM2",
            )
        )
    return records


def build_unicredit_december_2025_records() -> list[dict[str, object]]:
    records: list[dict[str, object]] = []

    for row_code, (row_name, value) in {
        "0010": ("1. Own funds and eligible liabilities", 90_655_000_000.0),
        "0020": ("EU-1a. Of which own funds and subordinated liabilities", 67_295_000_000.0),
        "0030": ("2. Total risk exposure amount of the resolution group TREA", 296_327_000_000.0),
        "0040": ("3. Own funds and eligible liabilities as a percentage of the TREA", 0.3059),
        "0050": ("EU-3a. Of which own funds and subordinated liabilities", 0.2271),
        "0060": ("4. Total exposure measure TEM of the resolution group", 906_925_000_000.0),
        "0070": ("5. Own funds and eligible liabilities as percentage of the TEM", 0.10),
        "0080": ("EU-5a. Of which own funds or subordinated liabilities", 0.0742),
        "0120": ("EU-7. MREL expressed as a percentage of the TREA", 0.2705),
        "0130": ("EU-8. Of which to be met with own funds or subordinated liabilities", 0.1936),
        "0140": ("EU-9. MREL expressed as a percentage of the TEM", 0.0598),
        "0150": ("EU-10. Of which to be met with own funds or subordinated liabilities", 0.0598),
    }.items():
        records.append(
            _base_record(
                entity_name="UniCredit S.p.A.",
                reference_date=DECEMBER_2025,
                template=KM2_TEMPLATE,
                row=row_code,
                row_name=row_name,
                value=value,
                module_code="PDF_KM2",
            )
        )

    for row_code, (row_name, value) in UNICREDIT_DEC_2025_TLAC1_ROWS.items():
        records.append(
            _base_record(
                entity_name="UniCredit S.p.A.",
                reference_date=DECEMBER_2025,
                template=TLAC1_TEMPLATE,
                row=row_code,
                row_name=row_name,
                value=value,
                module_code="PDF_TLAC1",
            )
        )

    for index, description in enumerate(UNICREDIT_DEC_2025_TLAC3_DESCRIPTIONS, start=1):
        records.append(
            _base_record(
                entity_name="UniCredit S.p.A.",
                reference_date=DECEMBER_2025,
                template=TLAC3_TEMPLATE,
                row="0010",
                row_name="1. Description of insolvency rank (free text)",
                value=description,
                column="0010",
                column_name=f"Rank {index}",
                module_code="PDF_TLAC3",
            )
        )

    for row_code, (row_name, values, total) in UNICREDIT_DEC_2025_TLAC3_VALUES.items():
        for index, value in enumerate(values, start=1):
            records.append(
                _base_record(
                    entity_name="UniCredit S.p.A.",
                    reference_date=DECEMBER_2025,
                    template=TLAC3_TEMPLATE,
                    row=row_code,
                    row_name=row_name,
                    value=value,
                    column="0010",
                    column_name=f"Rank {index}",
                    module_code="PDF_TLAC3",
                )
            )
        records.append(
            _base_record(
                entity_name="UniCredit S.p.A.",
                reference_date=DECEMBER_2025,
                template=TLAC3_TEMPLATE,
                row=row_code,
                row_name=row_name,
                value=total,
                column="0050",
                column_name="Sum of 1 to n",
                module_code="PDF_TLAC3",
            )
        )

    return records


def load_pdf_pillar3_supplement_long() -> pd.DataFrame:
    records: list[dict[str, object]] = []
    for (entity_name, reference_date), path in PDF_TEXT_KM2_SOURCES.items():
        if not path.exists():
            continue
        records.extend(build_km2_records(entity_name, path.read_text(), reference_date))

    records.extend(build_unicredit_december_2025_records())

    if not records:
        return _empty_frame()

    df = pd.DataFrame.from_records(records)
    return df[LONG_COLUMNS + ["fact_value_numeric"]].sort_values(
        ["entity_name", "reference_date", "template", "row", "column"]
    ).reset_index(drop=True)
