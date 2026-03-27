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
    capital_protection_pct: float | None = None  # e.g. 100.0
    original_amount: float | None = None  # EUR
    confidence: float = 1.0


ISIN_RE = re.compile(r"\b(IT|XS)\d{10}\b")

# Boilerplate sections to strip before checking subordination/SNP
# These are creditor hierarchy tables and BRRD disclosures present in ALL prospectuses
BOILERPLATE_PATTERNS = [
    # Creditor hierarchy table lines
    r"(?:debiti\s*chirografari|restanti\s*passivit[àa]|capitale\s*(?:di\s*)?classe|additional\s*tier)[^\n]{0,200}",
    # BRRD hierarchy disclosure paragraphs
    r"(?:gerarchia\s*dei\s*creditori|creditor\s*hierarchy|ordine\s*di\s*priorit[àa])[^\n]{0,500}",
    # "offerta subordinata a" = "offer subject to" (not a subordinated instrument)
    r"(?:offerta|domanda)\s*(?:(?:è|non)\s*)?subordinata?\s*(?:all|al|dovesse)",
    # "ivi incluse le obbligazioni subordinate" in hierarchy tables
    r"ivi\s*inclus[eai]\s*(?:le\s*)?obbligazioni\s*subordinat",
    # Generic hierarchy enumeration
    r"debiti\s*subordinati\s*diversi\s*(?:dal|dagli)",
    # "non subordinate" = NOT subordinated (pari passu clauses)
    r"(?:dirette|obbligazioni)[,\s]*non\s*subordinat[eai]",
    # "subordinata all'accoglimento" / "subordinata alla" = "subject to acceptance"
    r"subordinata?\s*(?:all['\u2019]|alla\s)",
    # Hierarchy descriptions mentioning senior non-preferred as a category
    r"obbligazioni\s*senior\s*non[- ]?preferred\)",  # In parenthetical descriptions
    # BRRD bail-out/-in paragraph (multi-line, strip generously)
    r"(?:bail[- ]?out|risorse\s*pubbliche|risoluzione\s*(?:delle?\s*)?crisi)[^.]{0,1000}\.",
    # "subordinata somma" / "nessuna subordinata" = "any/lesser sum" (not subordinated debt)
    r"(?:nessuna|alcuna)\s*\n?\s*subordinata?\s+(?:somma|importo)",
    # "detentori di titoli di debito subordinato" in hierarchy descriptions
    r"detentori\s*di\s*(?:titoli\s*di\s*)?debito\s*subordinat[^\n]{0,200}",
    # "l'offerta è subordinata" = "the offer is subject to" (conditional offer, not subordinated instrument)
    r"l[''\u2019]offerta\s*(?:è\s*)?(?:dovesse|subordinata)",
    # Creditor hierarchy table listing capital classes (CET1, AT1, T2)
    r"(?:common\s*equity|capitale\s*primario|strumenti\s*di\s*capitale)[^.]{0,800}(?:classe\s*[12]|tier\s*[12])",
]

# Title-level patterns for instrument self-description (high signal)
TITLE_SUBORDINATION_PATTERNS = [
    r"obbligazioni?\s+subordinat[eai]",
    r"(?:titoli?|notes?)\s+subordinat",
    r"\btier\s*[12]\b",
    r"\b[at][t]?[12]\b",
    r"subordinated\s+(?:notes?|bonds?|liabilities)",
    r"additional\s*tier\s*1",
]

TITLE_SNP_PATTERNS = [
    r"senior\s*non[- ]?preferred?\s+(?:notes?|bonds?|obbligazioni)",
    r"(?:notes?|bonds?|obbligazioni)\s+senior\s*non[- ]?preferred?",
    r"snp\s+(?:notes?|bonds?)",
]

TITLE_COVERED_BOND_PATTERNS = [
    r"obbligazioni?\s*bancari[ae]\s*garantit[ae]",
    r"covered\s*bond",
    r"\bobg\d*\b",
]

# General patterns (searched in full text, but only AFTER boilerplate stripping)
SUBORDINATION_PATTERNS = [
    r"(?<!un)(?<!non\s)subordinat[oaie]\b(?!\s*(?:all['']|al\b))",  # Exclude "non subordinate" and "subordinata all'accoglimento"
    r"\btier\s*2\b",
    r"\bclasse\s*subordinata\b",
    r"(?<!un)(?<!non[- ])subordinated\b",
]

SNP_PATTERNS = [
    r"senior\s*non[- ]?prefer",
    r"art(?:icolo|\.)\s*12[- ]?c",
    r"non[- ]?preferred\s*senior",
]

COVERED_BOND_PATTERNS = [
    r"obbligazioni?\s*bancari[ae]\s*garantit[ae]",
    r"covered\s*bond",
    r"\bobg\b",
]

CAPITAL_PROTECTION_PATTERNS = [
    r"protezione\s*del\s*capitale",
    r"capital\s*protect",
    r"rimborso\s*(?:minimo\s*)?(?:a\s*scadenza\s*)?(?:pari\s*al\s*)?100\s*%",
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
]

BAIL_IN_PATTERNS = [
    r"bail[- ]?in",
    r"brrd",
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


def _strip_boilerplate(text: str) -> str:
    """Remove known boilerplate sections that cause false positives."""
    cleaned = text
    for pattern in BOILERPLATE_PATTERNS:
        cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE)
    return cleaned


def _extract_title_section(text: str) -> str:
    """Extract the first ~2000 chars which typically contain the instrument title and description."""
    return text[:2000]


DATE_RE = re.compile(r"\b(\d{1,2})[/.](\d{1,2})[/.](\d{4})\b")

MONTH_MAP = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}


def _parse_english_date(s: str) -> str | None:
    """Parse '17 January 2020' to '17/01/2020'."""
    m = re.match(r"(\d{1,2})\s+(\w+)\s+(\d{4})", s.strip())
    if m:
        month = MONTH_MAP.get(m.group(2).lower())
        if month:
            return f"{m.group(1)}/{month}/{m.group(3)}"
    return None


def _extract_dates(text: str, data: ProspectusData) -> None:
    """Extract issue date and maturity date from prospectus text."""
    # Issue date patterns (in order of reliability)
    issue_patterns = [
        r"data\s*(?:di\s*)?emissione[:\s]*(\d{1,2}[/.]\d{1,2}[/.]\d{4})",
        r"data\s*di\s*godimento[:\s]*(\d{1,2}[/.]\d{1,2}[/.]\d{4})",
        # Table format: "Data di\n12.03.2025\nEmissione"
        r"data\s*di\s*\n?\s*(\d{1,2}[/.]\d{1,2}[/.]\d{4})\s*\n?\s*emissione",
    ]
    for pat in issue_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            data.issue_date = m.group(1)
            break

    # Maturity date patterns
    maturity_patterns = [
        r"data\s*(?:di\s*)?scadenza[:\s]*(\d{1,2}[/.]\d{1,2}[/.]\d{4})",
        r"data\s*di\s*\n?\s*(\d{1,2}[/.]\d{1,2}[/.]\d{4})\s*\n?\s*scadenza",
        r"scadenza[:\s]*(?:dei\s*(?:certificati|obbligazioni)\s*(?:è\s*)?)?(\d{1,2}[/.]\d{1,2}[/.]\d{4})",
        r"maturity\s*date[:\s]*(\d{1,2}[/.]\d{1,2}[/.]\d{4})",
    ]
    for pat in maturity_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            data.maturity_date = m.group(1)
            break

    # English date patterns
    if not data.issue_date:
        m = re.search(r"(?:issue\s*date|dated)[:\s]*(\d{1,2}\s+\w+\s+\d{4})", text[:5000], re.IGNORECASE)
        if m:
            data.issue_date = _parse_english_date(m.group(1))

    # Fallback: extract maturity from title (e.g., "Obbligazioni ... – 30.08.2030")
    if not data.maturity_date:
        title = text[:2000]
        m = re.search(r"[–\-]\s*(\d{1,2}[/.]\d{1,2}[/.]\d{4})", title)
        if m:
            data.maturity_date = m.group(1)


def _parse_amount_str(s: str) -> float | None:
    """Parse amounts in Italian (300.000.000) or English (400,000,000) format to float."""
    s = s.strip().replace(" ", "")
    # English format: commas as thousands separators (e.g., "400,000,000")
    if s.count(",") >= 2:
        s = s.replace(",", "")
    # Italian format: dots as thousands separators (e.g., "300.000.000")
    elif s.count(".") >= 2:
        s = s.replace(".", "")
    elif s.count(".") == 1:
        parts = s.split(".")
        if len(parts[1]) == 3:
            s = s.replace(".", "")
    elif s.count(",") == 1:
        parts = s.split(",")
        if len(parts[1]) == 3:
            s = s.replace(",", "")
        else:
            s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _extract_amounts(text: str, data: ProspectusData) -> None:
    """Extract nominal/original amount from prospectus text."""
    amount_patterns = [
        # "Ammontare Fino a 300.000.000 Euro" or "Ammontare Totale Fino a Euro 300.000.000"
        r"ammontare[^\n]{0,60}?(?:fino\s*a\s*)?(?:euro|eur|€)\s*([\d.]+)",
        r"ammontare[^\n]{0,60}?([\d.]+)\s*(?:euro|eur)",
        # "Importo Nominale Complessivo: Euro 300.000.000"
        r"importo\s*nominale\s*(?:complessivo)?[:\s]*(?:euro|eur|€)\s*([\d.]+)",
        # "aggregate nominal amount" (English)
        r"(?:aggregate|total)\s*(?:nominal\s*)?amount[:\s]*(?:eur|€)\s*([\d.,]+)",
        # "€400,000,000" near title
        r"€\s*([\d,]+(?:\.\d+)?)\s",
    ]
    for pat in amount_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            val = _parse_amount_str(m.group(1))
            if val and val >= 100_000:  # Minimum threshold to avoid parsing noise
                data.original_amount = val
                break


def _extract_capital_protection_pct(text: str, data: ProspectusData) -> None:
    """Extract capital protection percentage from prospectus."""
    protection_patterns = [
        # "Protezione 100%" in table format
        r"protezione\s*(\d{2,3})\s*%",
        # "Prezzo di Rimborso 100% del Valore Nominale"
        r"prezzo\s*(?:di\s*)?rimborso[:\s]*(\d{2,3})\s*%\s*(?:del\s*)?valore\s*nominale",
        # "rimborso a scadenza pari al 100%"
        r"rimborso\s*(?:a\s*scadenza\s*)?(?:pari\s*al?\s*)?(\d{2,3})\s*%",
        # "100% del Valore Nominale" near "rimborso" or "protezione"
        r"(\d{2,3})\s*%\s*del\s*valore\s*nominale\s*(?:unitario)?",
    ]
    for pat in protection_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            pct = float(m.group(1))
            if 50 <= pct <= 100:
                data.capital_protection_pct = pct
                break


def parse_prospectus(text: str) -> ProspectusData:
    data = ProspectusData()

    isin_match = ISIN_RE.search(text)
    if isin_match:
        data.isin = isin_match.group(0)

    text_lower = text.lower()
    if "eur" in text_lower or "\u20ac" in text_lower:
        data.currency = "EUR"

    # Extract title section for high-confidence classification
    title_section = _extract_title_section(text)

    # Strip boilerplate before checking subordination/SNP in full text
    cleaned_text = _strip_boilerplate(text)

    # Step 1: Check title-level patterns (highest confidence)
    title_sub, data.subordination_clause = _search_patterns(title_section, TITLE_SUBORDINATION_PATTERNS)
    title_snp, _ = _search_patterns(title_section, TITLE_SNP_PATTERNS)
    title_cb, _ = _search_patterns(title_section, TITLE_COVERED_BOND_PATTERNS)

    if title_sub or title_snp or title_cb:
        # High-confidence classification from title
        data.is_subordinated = title_sub
        data.is_senior_non_preferred = title_snp
        data.is_covered_bond = title_cb
    else:
        # Step 2: Fall back to cleaned full text for SNP and covered bonds
        # Subordination is ONLY from title — full-text "subordinat*" matches produce
        # too many false positives (BRRD boilerplate, hierarchy tables, PDF artifacts)
        data.is_senior_non_preferred, _ = _search_patterns(cleaned_text, SNP_PATTERNS)
        data.is_covered_bond, _ = _search_patterns(cleaned_text, COVERED_BOND_PATTERNS)

    # These signals are safe to search in the full text (no boilerplate issues)
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

    # Extract structured data: dates, amounts, protection %
    _extract_dates(text, data)
    _extract_amounts(text, data)
    _extract_capital_protection_pct(text, data)

    # Confidence scoring
    has_title_signal = title_sub or title_snp or title_cb
    signals = [
        data.is_subordinated, data.is_senior_non_preferred, data.is_covered_bond,
        data.is_capital_protected, data.is_underlying_linked, data.has_barrier,
        data.has_autocallable,
    ]
    if has_title_signal:
        data.confidence = 0.95
    elif data.is_capital_protected and data.has_barrier:
        data.confidence = 0.6
    elif sum(signals) == 0:
        data.confidence = 0.5
    else:
        data.confidence = 0.8

    return data
