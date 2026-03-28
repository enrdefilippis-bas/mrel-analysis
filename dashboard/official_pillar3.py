from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

import pandas as pd

try:
    import streamlit as st
except ModuleNotFoundError:  # pragma: no cover - fallback for bare Python execution
    class _StreamlitCacheShim:
        @staticmethod
        def cache_data(ttl: int | None = None):
            def decorator(func):
                return func
            return decorator

    st = _StreamlitCacheShim()

WORKBOOK_PATH = Path(__file__).resolve().parent.parent / "datapillar325.xlsx"

KM2_TEMPLATE = (
    "K_90.01 - EU KM2 - Key metrics - MREL and, where applicable, "
    "G-SII requirement for own funds and eligible liabilities"
)
TLAC1_TEMPLATE = (
    "K_91.00 - EU TLAC1 - Composition - MREL and, where applicable, "
    "G-SII requirement for own funds and eligible liabilities"
)
TLAC3_TEMPLATE = "K_97.00 - EU TLAC3 - creditor ranking - resolution entity"

TEMPLATE_MAP = {
    "KM2": KM2_TEMPLATE,
    "TLAC1": TLAC1_TEMPLATE,
    "TLAC3": TLAC3_TEMPLATE,
}

BANK_LOGO_DOMAINS = {
    "ABN AMRO Bank N.V.": "abnamro.com",
    "AIB Group plc": "aib.ie",
    "ALIOR BANK S.A.": "aliorbank.pl",
    "BANCA MEDIOLANUM S.P.A.": "bancamediolanum.it",
    "BANCA POPOLARE DI SONDRIO SOCIETA' PER AZIONI": "popso.it",
    "BANCO BPM SOCIETA' PER AZIONI": "bancobpm.it",
    "BNP Paribas": "bnpparibas.com",
    "Banca Monte dei Paschi di Siena S.p.A.": "mps.it",
    "Banco Bilbao Vizcaya Argentaria, S.A.": "bbva.com",
    "Banco Santander, S.A.": "santander.com",
    "Banco de Sabadell, S.A.": "bancsabadell.com",
    "Bank Polska Kasa Opieki S.A.": "pekao.com.pl",
    "Bank of Ireland Group plc": "bankofireland.com",
    "Bankinter, S.A.": "bankinter.com",
    "CASSA CENTRALE BANCA - CREDITO COOPERATIVO ITALIANOSOCIETA' PER AZIONI (IN SIGLA CASSA CENTRALE BANCA)": "cassacentrale.it",
    "COMMERZBANK Aktiengesellschaft": "commerzbank.com",
    "CREDITO EMILIANO HOLDING SOCIETA' PER AZIONI": "credem.it",
    "Confédération Nationale du Crédit Mutuel": "creditmutuel.com",
    "Coöperatieve Rabobank U.A.": "rabobank.com",
    "DEUTSCHE BANK AKTIENGESELLSCHAFT": "db.com",
    "DNB BANK ASA": "dnb.no",
    "Groupe BPCE": "groupebpce.com",
    "ICCREA BANCA S.P.A. - ISTITUTO CENTRALE DEL CREDITO COOPERATIVO (IN FORMA ABBREVIATA: ICCREA BANCA S.P.A.)": "iccreabanca.it",
    "ING Belgie": "ing.be",
    "ING Groep N.V.": "ing.com",
    "Ibercaja Banco, S.A.": "ibercaja.es",
    "Investeringsmaatschappij Argenta - Société d'investissements Argenta - Investierungsgesellschaft Arg": "argenta.be",
    "KBC Groupe": "kbc.com",
    "Kutxabank, S.A.": "kutxabank.es",
    "MEDIOBANCA PREMIER S.P.A.": "mediobancapremier.com",
    "Mediobanca - Banca di Credito Finanziario S.p.A.": "mediobanca.com",
    "Nordea Bank Abp": "nordea.com",
    "Powszechna Kasa Oszczednosci Bank Polski S.A.": "pkobp.pl",
    "Santander Bank Polska S.A.": "santander.pl",
    "Swedbank - Grupp": "swedbank.com",
    "Tatra banka, a.s.": "tatrabanka.sk",
    "Triodos Bank N.V.": "triodos.com",
    "de Volksbank N.V.": "devolksbank.nl",
    "mBank S.A.": "mbank.pl",
    "Íslandsbanki hf.": "islandsbanki.is",
    "Česká spořitelna, a.s.": "csas.cz",
}

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


@dataclass(frozen=True)
class OfficialWaterfallData:
    components: tuple[dict[str, object], ...]
    requirement_lines: tuple[dict[str, object], ...]
    total_mrel: float
    subordination_total: float
    trea: float


@dataclass(frozen=True)
class NormalizedRequirementProfile:
    actual_mrel_trea: float | None
    actual_subordination_trea: float | None
    actual_mrel_tem: float | None
    actual_subordination_tem: float | None
    requirement_mrel_trea_raw: float | None
    requirement_subordination_trea_raw: float | None
    requirement_mrel_tem_raw: float | None
    requirement_subordination_tem_raw: float | None
    requirement_mrel_trea: float | None
    requirement_subordination_trea: float | None
    requirement_mrel_tem: float | None
    requirement_subordination_tem: float | None
    cbr_trea: float | None
    binding_mrel_trea: float | None
    cbr_disclosed: bool
    ratio_scale_notes: tuple[str, ...]


def get_bank_logo_url(entity_name: str) -> str | None:
    domain = BANK_LOGO_DOMAINS.get(entity_name)
    if not domain:
        return None
    return f"https://www.google.com/s2/favicons?domain_url=https://{domain}&sz=128"


def get_bank_monogram(entity_name: str) -> str:
    tokens = re.findall(r"[A-Za-z0-9À-ÖØ-öø-ÿ]+", entity_name)
    if not tokens:
        return "BK"
    letters = "".join(token[0] for token in tokens[:2]).upper()
    return letters[:2] if letters else "BK"


def _empty_long_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=LONG_COLUMNS)


def _clean_text(value: object) -> object:
    if isinstance(value, str):
        return " ".join(value.split())
    return value


def _read_official_workbook(path: Path) -> pd.DataFrame:
    if not path.exists():
        return _empty_long_frame()

    raw = pd.read_excel(path, sheet_name="Export", header=None)
    if raw.empty or len(raw) < 3:
        return _empty_long_frame()

    metadata_headers = raw.iloc[1, :13].tolist()
    date_headers = [
        pd.Timestamp(value).date().isoformat()
        for value in raw.iloc[0, 13:].tolist()
        if pd.notna(value)
    ]
    all_headers = metadata_headers + date_headers

    normalized = raw.iloc[2:, : len(all_headers)].copy()
    normalized.columns = all_headers
    normalized = normalized.rename(
        columns={
            "Entity Code": "entity_code",
            "Entity Name": "entity_name",
            "Country": "country",
            "Module Name": "module_name",
            "ModuleCode": "module_code",
            "Cell": "cell",
            "Open Key": "open_key",
            "Template": "template",
            "Row": "row",
            "Row Name": "row_name",
            "Column": "column",
            "Column Name": "column_name",
            "Sheet": "sheet",
        }
    )

    long_df = normalized.melt(
        id_vars=[
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
        ],
        value_vars=date_headers,
        var_name="reference_date",
        value_name="fact_value",
    )
    long_df = long_df.dropna(subset=["fact_value"]).copy()

    for column in ["entity_code", "entity_name", "country", "module_name", "module_code", "cell",
                   "template", "row", "row_name", "column", "column_name", "sheet"]:
        if column in long_df.columns:
            long_df[column] = long_df[column].map(_clean_text)

    long_df["reference_date"] = long_df["reference_date"].astype(str)
    long_df["fact_value_numeric"] = pd.to_numeric(long_df["fact_value"], errors="coerce")
    long_df["row"] = long_df["row"].astype("string")
    long_df["column"] = long_df["column"].astype("string")

    return long_df[LONG_COLUMNS + ["fact_value_numeric"]].sort_values(
        ["entity_name", "reference_date", "template", "row", "column"]
    )


@st.cache_data(ttl=300)
def load_official_pillar3_long(path_str: str | None = None) -> pd.DataFrame:
    path = Path(path_str) if path_str else WORKBOOK_PATH
    return _read_official_workbook(path)


@st.cache_data(ttl=300)
def list_official_banks(path_str: str | None = None) -> list[str]:
    df = load_official_pillar3_long(path_str)
    return sorted(df["entity_name"].dropna().unique().tolist())


@st.cache_data(ttl=300)
def list_supported_reference_dates(path_str: str | None = None) -> list[str]:
    df = load_official_pillar3_long(path_str)
    return sorted(df["reference_date"].dropna().unique().tolist())


@st.cache_data(ttl=300)
def list_bank_dates(entity_name: str, path_str: str | None = None) -> list[str]:
    df = load_official_pillar3_long(path_str)
    subset = df[df["entity_name"] == entity_name]
    return sorted(subset["reference_date"].dropna().unique().tolist())


@st.cache_data(ttl=300)
def get_template_coverage(entity_name: str, reference_date: str, path_str: str | None = None) -> dict[str, bool]:
    df = load_official_pillar3_long(path_str)
    subset = df[(df["entity_name"] == entity_name) & (df["reference_date"] == reference_date)]
    templates = set(subset["template"].dropna().tolist())
    return {key: template in templates for key, template in TEMPLATE_MAP.items()}


@st.cache_data(ttl=300)
def get_template_snapshot(
    entity_name: str,
    reference_date: str,
    template_key: str,
    path_str: str | None = None,
) -> pd.DataFrame:
    df = load_official_pillar3_long(path_str)
    template_name = TEMPLATE_MAP[template_key]
    snapshot = df[
        (df["entity_name"] == entity_name)
        & (df["reference_date"] == reference_date)
        & (df["template"] == template_name)
    ].copy()
    return snapshot.sort_values(["row", "column", "row_name", "column_name"]).reset_index(drop=True)


def _numeric_sum(snapshot: pd.DataFrame, row_codes: list[str]) -> float:
    if snapshot.empty:
        return 0.0
    numeric = snapshot[snapshot["row"].isin(row_codes)]["fact_value_numeric"].dropna()
    if numeric.empty:
        return 0.0
    return float(numeric.sum())


def _numeric_first(snapshot: pd.DataFrame, row_code: str) -> float | None:
    if snapshot.empty:
        return None
    numeric = snapshot[snapshot["row"] == row_code]["fact_value_numeric"].dropna()
    if numeric.empty:
        return None
    return float(numeric.iloc[0])


_RATIO_SCALE_CANDIDATES = (
    1.0,
    10.0,
    100.0,
    1_000.0,
    10_000.0,
    100_000.0,
    1_000_000.0,
    10_000_000.0,
    100_000_000.0,
    1_000_000_000.0,
    10_000_000_000.0,
)


def _normalize_ratio_general(raw_value: float | None, upper_bound: float = 1.0) -> tuple[float | None, float | None]:
    if raw_value is None or pd.isna(raw_value):
        return None, None

    raw = float(raw_value)
    for scale in _RATIO_SCALE_CANDIDATES:
        normalized = raw / scale
        if abs(normalized) <= upper_bound:
            return normalized, scale

    return None, None


def _normalize_ratio_from_amount(
    raw_value: float | None,
    numerator: float | None,
    denominator: float | None,
    upper_bound: float = 1.5,
) -> tuple[float | None, float | None]:
    if raw_value is None or numerator in (None, 0) or denominator in (None, 0):
        return _normalize_ratio_general(raw_value, upper_bound=upper_bound)

    raw = float(raw_value)
    expected = float(numerator) / float(denominator)
    candidates: list[tuple[float, float, float]] = []
    for scale in _RATIO_SCALE_CANDIDATES:
        normalized = raw / scale
        if abs(normalized) <= upper_bound:
            candidates.append((abs(normalized - expected), scale, normalized))

    if not candidates:
        return _normalize_ratio_general(raw_value, upper_bound=upper_bound)

    _, best_scale, best_value = min(candidates, key=lambda item: (item[0], item[1]))
    return best_value, best_scale


def _normalize_row_ratio(
    snapshot: pd.DataFrame,
    row_code: str,
    numerator: float | None = None,
    denominator: float | None = None,
    upper_bound: float = 1.0,
) -> tuple[float | None, float | None]:
    raw = _numeric_first(snapshot, row_code)
    if numerator is not None and denominator is not None:
        return _normalize_ratio_from_amount(raw, numerator, denominator, upper_bound=max(upper_bound, 1.5))
    return _normalize_ratio_general(raw, upper_bound=upper_bound)


@st.cache_data(ttl=300)
def get_normalized_requirement_profile(
    entity_name: str,
    reference_date: str,
    path_str: str | None = None,
) -> NormalizedRequirementProfile:
    km2 = get_template_snapshot(entity_name, reference_date, "KM2", path_str)
    tlac1 = get_template_snapshot(entity_name, reference_date, "TLAC1", path_str)

    total_mrel_amount = _numeric_first(km2, "0010")
    subordinated_amount = _numeric_first(km2, "0020")
    trea_amount = _numeric_first(km2, "0030")
    tem_amount = _numeric_first(km2, "0060")

    actual_mrel_trea, scale_actual_mrel_trea = _normalize_row_ratio(km2, "0040", total_mrel_amount, trea_amount)
    actual_sub_trea, scale_actual_sub_trea = _normalize_row_ratio(km2, "0050", subordinated_amount, trea_amount)
    actual_mrel_tem, scale_actual_mrel_tem = _normalize_row_ratio(km2, "0070", total_mrel_amount, tem_amount)
    actual_sub_tem, scale_actual_sub_tem = _normalize_row_ratio(km2, "0080", subordinated_amount, tem_amount)

    req_mrel_trea_raw = _numeric_first(km2, "0120")
    req_sub_trea_raw = _numeric_first(km2, "0130")
    req_mrel_tem_raw = _numeric_first(km2, "0140")
    req_sub_tem_raw = _numeric_first(km2, "0150")

    req_mrel_trea, scale_req_mrel_trea = _normalize_ratio_general(req_mrel_trea_raw, upper_bound=1.0)
    req_sub_trea, scale_req_sub_trea = _normalize_ratio_general(req_sub_trea_raw, upper_bound=1.0)
    req_mrel_tem, scale_req_mrel_tem = _normalize_ratio_general(req_mrel_tem_raw, upper_bound=1.0)
    req_sub_tem, scale_req_sub_tem = _normalize_ratio_general(req_sub_tem_raw, upper_bound=1.0)

    cbr_raw = _numeric_first(tlac1[tlac1["column"] == "0020"], "0340")
    cbr_trea, scale_cbr = _normalize_ratio_general(cbr_raw, upper_bound=0.25)
    binding_mrel_trea = (req_mrel_trea + cbr_trea) if req_mrel_trea is not None and cbr_trea is not None else req_mrel_trea

    notes: list[str] = []
    scale_map = {
        "KM2 actual MREL/TREA": scale_actual_mrel_trea,
        "KM2 actual subordination/TREA": scale_actual_sub_trea,
        "KM2 actual MREL/TEM": scale_actual_mrel_tem,
        "KM2 actual subordination/TEM": scale_actual_sub_tem,
        "KM2 requirement MREL/TREA": scale_req_mrel_trea,
        "KM2 requirement subordination/TREA": scale_req_sub_trea,
        "KM2 requirement MREL/TEM": scale_req_mrel_tem,
        "KM2 requirement subordination/TEM": scale_req_sub_tem,
        "CBR/TREA": scale_cbr,
    }
    for label, scale in scale_map.items():
        if scale and scale != 1.0:
            notes.append(f"{label} rescaled by /{scale:,.0f}".replace(",", "."))

    return NormalizedRequirementProfile(
        actual_mrel_trea=actual_mrel_trea,
        actual_subordination_trea=actual_sub_trea,
        actual_mrel_tem=actual_mrel_tem,
        actual_subordination_tem=actual_sub_tem,
        requirement_mrel_trea_raw=req_mrel_trea_raw,
        requirement_subordination_trea_raw=req_sub_trea_raw,
        requirement_mrel_tem_raw=req_mrel_tem_raw,
        requirement_subordination_tem_raw=req_sub_tem_raw,
        requirement_mrel_trea=req_mrel_trea,
        requirement_subordination_trea=req_sub_trea,
        requirement_mrel_tem=req_mrel_tem,
        requirement_subordination_tem=req_sub_tem,
        cbr_trea=cbr_trea,
        binding_mrel_trea=binding_mrel_trea,
        cbr_disclosed=cbr_trea is not None,
        ratio_scale_notes=tuple(notes),
    )


def build_official_waterfall(
    entity_name: str,
    reference_date: str,
    path_str: str | None = None,
) -> OfficialWaterfallData | None:
    tlac1 = get_template_snapshot(entity_name, reference_date, "TLAC1", path_str)
    profile = get_normalized_requirement_profile(entity_name, reference_date, path_str)

    if tlac1.empty:
        return None

    cet1 = _numeric_first(tlac1, "0010") or 0.0
    at1 = _numeric_first(tlac1, "0020") or 0.0
    tier2 = _numeric_first(tlac1, "0060") or 0.0
    subordinated = _numeric_sum(tlac1, ["0100", "0110", "0120", "0130"])
    non_subordinated = _numeric_first(tlac1, "0160") or 0.0
    mpe_deduction = _numeric_first(tlac1, "0220") or 0.0
    investment_deduction = _numeric_first(tlac1, "0230") or 0.0
    total_mrel = _numeric_first(tlac1, "0250") or 0.0
    trea = _numeric_first(get_template_snapshot(entity_name, reference_date, "KM2", path_str), "0030") or 0.0
    mrel_req_ratio = profile.binding_mrel_trea
    sub_req_ratio = profile.requirement_subordination_trea

    running_total = (
        cet1
        + at1
        + tier2
        + subordinated
        + non_subordinated
        - mpe_deduction
        - investment_deduction
    )
    residual_adjustment = total_mrel - running_total

    components: list[dict[str, object]] = [
        {"label": "CET1", "value": cet1, "measure": "relative"},
        {"label": "AT1", "value": at1, "measure": "relative"},
        {"label": "Tier 2", "value": tier2, "measure": "relative"},
        {"label": "Subordinated Eligible Liabilities", "value": subordinated, "measure": "relative"},
        {"label": "Non-Subordinated Eligible Liabilities", "value": non_subordinated, "measure": "relative"},
    ]

    if mpe_deduction:
        components.append({"label": "MPE / Other Deductions", "value": -mpe_deduction, "measure": "relative"})
    if investment_deduction:
        components.append(
            {"label": "Investments in Eligible Liabilities", "value": -investment_deduction, "measure": "relative"}
        )
    if abs(residual_adjustment) >= 1:
        components.append({"label": "Residual Regulatory Adjustment", "value": residual_adjustment, "measure": "relative"})

    components.append({"label": "Total MREL", "value": total_mrel, "measure": "total"})

    requirement_lines: tuple[dict[str, object], ...] = ()
    if trea and mrel_req_ratio is not None and sub_req_ratio is not None:
        requirement_lines = (
            {
                "label": "MREL requirement",
                "value": trea * mrel_req_ratio,
                "color": "red",
                "dash": "dash",
                "annotation": (
                    f"MREL Req + CBR ({mrel_req_ratio * 100:.2f}% TREA)"
                    if profile.cbr_disclosed
                    else f"MREL Req ({mrel_req_ratio * 100:.2f}% TREA)"
                ),
            },
            {
                "label": "Subordination requirement",
                "value": trea * sub_req_ratio,
                "color": "orange",
                "dash": "dot",
                "annotation": f"Sub. Req ({sub_req_ratio * 100:.2f}% TREA)",
            },
        )

    return OfficialWaterfallData(
        components=tuple(components),
        requirement_lines=requirement_lines,
        total_mrel=total_mrel,
        subordination_total=cet1 + at1 + tier2 + subordinated,
        trea=trea,
    )


def build_tlac3_rank_table(entity_name: str, reference_date: str, path_str: str | None = None) -> pd.DataFrame:
    snapshot = get_template_snapshot(entity_name, reference_date, "TLAC3", path_str)
    if snapshot.empty:
        return pd.DataFrame()

    per_rank = snapshot[snapshot["column"] == "0010"].copy()
    if per_rank.empty:
        return pd.DataFrame()

    descriptions_raw = per_rank[per_rank["row"] == "0010"]["fact_value"].dropna().tolist()
    if not descriptions_raw:
        return pd.DataFrame()

    descriptions = pd.DataFrame(
        {
            "rank_index": list(range(1, len(descriptions_raw) + 1)),
            "Description": descriptions_raw,
        }
    )

    def assign_values(row_code: str, ceilings: list[float | None] | None = None) -> list[float | None]:
        values = per_rank[per_rank["row"] == row_code]["fact_value_numeric"].dropna().tolist()
        if not values:
            return [None] * len(descriptions)

        assigned: list[float | None] = [None] * len(descriptions)
        next_rank = 0
        tolerance = 1e-6

        for value in values:
            placed = False
            for idx in range(next_rank, len(descriptions)):
                ceiling = None if ceilings is None else ceilings[idx]
                if ceiling is None or value <= float(ceiling) + tolerance:
                    assigned[idx] = float(value)
                    next_rank = idx + 1
                    placed = True
                    break
            if not placed:
                for idx in range(next_rank, len(descriptions)):
                    if assigned[idx] is None:
                        assigned[idx] = float(value)
                        next_rank = idx + 1
                        placed = True
                        break
            if not placed:
                assigned[-1] = float(value)

        return assigned

    row_map = {
        "0020": "Liabilities and Own Funds",
        "0030": "Excluded Liabilities",
        "0040": "Less Excluded Liabilities",
        "0050": "Potentially Eligible for MREL/TLAC",
    }

    table = descriptions.copy()
    table["Rank"] = table["rank_index"].map(lambda idx: f"Rank {idx}")
    liabilities = assign_values("0020")
    excluded = assign_values("0030", ceilings=liabilities)
    less_excluded = assign_values("0040", ceilings=liabilities)
    potentially_eligible = assign_values("0050", ceilings=less_excluded)

    table["Liabilities and Own Funds"] = liabilities
    table["Excluded Liabilities"] = excluded
    table["Less Excluded Liabilities"] = less_excluded
    table["Potentially Eligible for MREL/TLAC"] = potentially_eligible

    totals = snapshot[snapshot["column"] == "0050"].copy()
    if not totals.empty:
        total_row = {"Rank": "Total", "Description": "Sum of 1 to n"}
        for row_code, label in row_map.items():
            total_row[label] = _numeric_first(totals, row_code)
        table = pd.concat([table, pd.DataFrame([total_row])], ignore_index=True)

    return table[
        [
            "Rank",
            "Description",
            "Liabilities and Own Funds",
            "Excluded Liabilities",
            "Less Excluded Liabilities",
            "Potentially Eligible for MREL/TLAC",
        ]
    ]
