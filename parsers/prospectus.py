from __future__ import annotations
import re
from dataclasses import dataclass


@dataclass
class ProspectusData:
    isin: str | None = None
    instrument_name: str | None = None
    issue_date: str | None = None
    maturity_date: str | None = None
    nominal_amount: str | None = None
    coupon_type: str | None = None
    coupon_rate: str | None = None
    currency: str | None = None
    is_subordinated: bool = False
    is_senior_non_preferred: bool = False
    is_covered_bond: bool = False
    is_capital_protected: bool = False
    is_underlying_linked: bool = False
    has_barrier: bool = False
    has_autocallable: bool = False
    has_bail_in_clause: bool = False
    subordination_clause: str | None = None
    bail_in_clause: str | None = None
    capital_protection_clause: str | None = None
    payoff_clause: str | None = None
    confidence: float = 1.0


ISIN_RE = re.compile(r"\b(IT|XS)\d{10}\b")

SUBORDINATION_PATTERNS = [
    r"subordinat[oaie]",
    r"tier\s*2",
    r"classe\s*subordinata",
    r"subordinated",
]

SNP_PATTERNS = [
    r"senior\s*non[- ]?prefer",
    r"art(?:icolo|\.)\s*12[- ]?c",
    r"crediti\s*di\s*secondo\s*livello",
    r"non[- ]?preferred\s*senior",
]

COVERED_BOND_PATTERNS = [
    r"obbligazioni?\s*bancari[ae]\s*garantit[ae]",
    r"covered\s*bond",
    r"obg",
]

CAPITAL_PROTECTION_PATTERNS = [
    r"protezione\s*del\s*capitale",
    r"capital\s*protect",
    r"rimborso\s*(?:minimo\s*)?(?:a\s*scadenza\s*)?(?:pari\s*al\s*)?100\s*%",
    r"valore\s*nominale\s*a\s*scadenza",
    r"rimborso\s*integrale\s*del\s*(?:valore\s*)?nominale",
    r"100%\s*del\s*valore\s*nominale",
]

UNDERLYING_LINKED_PATTERNS = [
    r"sottostan(?:te|ti)",
    r"underlying",
    r"indice\s*di\s*riferimento",
    r"linked\s*to",
    r"basket",
    r"azioni?\s*sottostan",
]

BARRIER_PATTERNS = [
    r"barriera",
    r"barrier",
    r"knock[- ]?(?:in|out)",
    r"livello\s*(?:di\s*)?barriera",
]

AUTOCALLABLE_PATTERNS = [
    r"autocall",
    r"rimborso\s*anticipato\s*(?:automatico|condizionato)",
    r"early\s*redemption\s*(?:automatic|conditional)",
    r"callable\s*(?:su|on)\s*(?:base|basis)",
]

BAIL_IN_PATTERNS = [
    r"bail[- ]?in",
    r"risoluzione",
    r"resolution",
    r"brrd",
    r"art(?:icolo|\.)\s*44",
    r"svalutazione\s*e\s*(?:di\s*)?conversione",
    r"write[- ]?down",
]


def _search_patterns(text: str, patterns: list[str]) -> tuple[bool, str | None]:
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            start = max(0, match.start() - 100)
            end = min(len(text), match.end() + 100)
            context = text[start:end].strip()
            return True, context
    return False, None


def parse_prospectus(text: str) -> ProspectusData:
    data = ProspectusData()

    isin_match = ISIN_RE.search(text)
    if isin_match:
        data.isin = isin_match.group(0)

    text_lower = text.lower()
    if "eur" in text_lower or "\u20ac" in text_lower:
        data.currency = "EUR"

    data.is_subordinated, data.subordination_clause = _search_patterns(
        text, SUBORDINATION_PATTERNS
    )
    data.is_senior_non_preferred, _ = _search_patterns(text, SNP_PATTERNS)
    data.is_covered_bond, _ = _search_patterns(text, COVERED_BOND_PATTERNS)
    data.is_capital_protected, data.capital_protection_clause = _search_patterns(
        text, CAPITAL_PROTECTION_PATTERNS
    )
    data.is_underlying_linked, data.payoff_clause = _search_patterns(
        text, UNDERLYING_LINKED_PATTERNS
    )
    data.has_barrier, _ = _search_patterns(text, BARRIER_PATTERNS)
    data.has_autocallable, _ = _search_patterns(text, AUTOCALLABLE_PATTERNS)
    data.has_bail_in_clause, data.bail_in_clause = _search_patterns(
        text, BAIL_IN_PATTERNS
    )

    signals = [
        data.is_subordinated,
        data.is_senior_non_preferred,
        data.is_covered_bond,
        data.is_capital_protected,
        data.is_underlying_linked,
        data.has_barrier,
        data.has_autocallable,
    ]
    if data.is_capital_protected and data.has_barrier:
        data.confidence = 0.6
    elif sum(signals) == 0:
        data.confidence = 0.5
    else:
        data.confidence = 0.9

    return data
