# MREL Analysis — Banco BPM — Design Document

**Date:** 2026-03-26
**Reference Date:** 31.12.2024
**Scope:** Banco BPM only (proof of concept)
**Approach:** Prospectus-First (bottom-up instrument classification, reconciled against Pillar 3)

---

## 1. Objective

Build an interactive Streamlit dashboard that calculates MREL capacity for Banco BPM at the instrument level, solving the core problem that senior bonds and certificates share the same insolvency rank (5) and cannot be distinguished using Pillar 3 data alone.

The tool scrapes prospectuses to classify each instrument based on actual contractual terms, cross-references ISINs against public sources for outstanding amounts, and reconciles bottom-up totals against Pillar 3 aggregates.

---

## 2. Data Pipeline Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    DATA COLLECTION                        │
│                                                          │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────┐  │
│  │ Banco BPM   │  │ Borsa        │  │ ECB CSDB /     │  │
│  │ IR Website  │  │ Italiana     │  │ Pillar 3       │  │
│  │ (Prospectus)│  │ (ISINs/Amts) │  │ (Aggregates)   │  │
│  └──────┬──────┘  └──────┬───────┘  └───────┬────────┘  │
│         │                │                   │           │
└─────────┼────────────────┼───────────────────┼───────────┘
          │                │                   │
          ▼                ▼                   ▼
┌──────────────────────────────────────────────────────────┐
│                    PROCESSING                            │
│                                                          │
│  1. Parse prospectus PDFs → extract ISINs, terms,        │
│     MREL clauses, maturity, notional                     │
│  2. Cross-reference ISINs with Borsa Italiana            │
│  3. Classify each instrument per CRR2/BRRD2/SRB rules   │
│  4. Compute residual maturity from 31.12.2024            │
│  5. Build MREL stack (subordination, total, CBR)         │
│  6. Reconcile against Pillar 3 aggregates                │
│                                                          │
└──────────────────────┬───────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────┐
│                 STREAMLIT DASHBOARD                       │
│                                                          │
│  • Instrument table (filter by type, maturity, eligible) │
│  • MREL stack waterfall chart                            │
│  • Reconciliation view (bottom-up vs Pillar 3)           │
│  • Export to Excel                                       │
└──────────────────────────────────────────────────────────┘
```

### Project Structure

```
mrel-analysis/
├── scrapers/          # One scraper per data source
│   ├── banco_bpm.py       # Prospectus & IR page scraper
│   ├── borsa_italiana.py  # ISIN lookup & outstanding amounts
│   └── pillar3.py         # Pillar 3 PDF table extraction
├── parsers/           # PDF parsing & classification
│   ├── pdf_parser.py      # PDF text extraction (PyMuPDF/pdfplumber)
│   ├── prospectus.py      # Prospectus clause extraction
│   └── classifier.py      # MREL eligibility classification engine
├── models/            # Data models
│   ├── instrument.py      # Instrument dataclass
│   ├── mrel_stack.py      # MREL stack computation
│   └── eligibility.py     # Eligibility criteria per CRR2/BRRD2
├── dashboard/         # Streamlit app
│   ├── app.py             # Main entry point
│   ├── views/
│   │   ├── explorer.py        # Instrument Explorer view
│   │   ├── waterfall.py       # MREL Stack Waterfall view
│   │   ├── reconciliation.py  # Reconciliation view
│   │   └── audit.py           # Data Quality & Audit view
│   └── components/
│       └── charts.py      # Reusable chart components
├── data/              # Local data storage
│   ├── raw/               # Downloaded PDFs
│   ├── processed/         # Parsed instrument data
│   └── db/                # SQLite database
├── docs/plans/        # Design & planning docs
├── requirements.txt
└── README.md
```

---

## 3. Instrument Classification Logic

### CRR2 Article 72a Eligibility Criteria

For each instrument, we check:

1. Issued and fully paid up (not self-purchased)
2. Not owed to / guaranteed by the institution
3. Residual maturity >= 1 year from reference date
4. Not arising from derivatives
5. Not arising from deposits with insolvency preference
6. Contractual bail-in recognition clause present (for non-EU law instruments)
7. Not subject to set-off or netting undermining loss absorption

### Classification Buckets

| Category | CRR2 Rank | MREL Layer | Identification Method |
|---|---|---|---|
| CET1 | 1 | Subordination + Total | Pillar 3 only |
| AT1 | 2 | Subordination + Total | Pillar 3 + prospectus (perpetual, coupon skip) |
| Tier 2 | 3 | Subordination + Total | Prospectus: explicit subordination clause |
| Senior Non-Preferred | 4 | Subordination + Total | Prospectus: "senior non preferred" / Art. 12c CRR2 |
| Senior Preferred | 5 | Total MREL only | Prospectus: senior, plain vanilla coupon, no structured payoff |
| Structured Notes (protected) | 5 | **Total MREL** | Prospectus: linked to underlying BUT principal guaranteed at par at maturity |
| Certificates (non-protected) | 5 | **Excluded** | Prospectus: principal at risk, barrier, autocallable |
| Covered Bonds | — | **Excluded** | Prospectus: "obbligazioni bancarie garantite" |

### Rank 5 Three-Way Discrimination

The core problem: senior bonds, capital-protected structured notes, and certificates all share rank 5. We discriminate by parsing prospectus language:

1. **Plain vanilla senior** → MREL eligible (Total): fixed/floating coupon, bullet/amortising, no linked payoff
2. **Structured notes with capital protection** → MREL eligible (Total): payoff linked to underlying but principal repayment >= 100% at maturity regardless of underlying performance. Keywords: "protezione del capitale", "rimborso minimo a scadenza pari al 100%"
3. **Certificates / non-protected structured** → Excluded: principal at risk, barrier features, autocallable. Keywords: "autocallable", "barrier", "quanto", formula-based redemption

### Maturity Bucketing

- Residual maturity > 1 year → fully eligible
- Residual maturity <= 1 year → excluded
- Perpetual instruments → eligible (subject to call treatment per SRB policy)

---

## 4. Data Sources & Scraping Strategy

### Source 1: Banco BPM Investor Relations
- **URL:** `https://gruppo.bancobpm.it` → IR → Debt Issuance / EMTN Programme
- **Content:** Base Prospectus PDFs, Final Terms PDFs, EMTN supplements
- **Approach:** Scrape index pages for PDF links, download all Final Terms (one per ISIN)
- **Tools:** httpx (async), BeautifulSoup (HTML), PyMuPDF/pdfplumber (PDF)

### Source 2: Borsa Italiana
- **URL:** `https://www.borsaitaliana.it` → search by issuer "Banco BPM"
- **Content:** ISIN list, outstanding amounts, instrument descriptions, listing segment
- **Purpose:** Universe of listed instruments, validation of prospectus data

### Source 3: Pillar 3 Report (31.12.2024)
- **URL:** Banco BPM IR page → Pillar 3 Disclosures
- **Key tables:** EU TLAC1, EU TLAC1a, TLAC3
- **Purpose:** Reconciliation (aggregate MREL capacity by category)

### Source 4 (fallback): ECB CSDB
- Centralised Securities Database for outstanding amounts
- Public access may be limited — assess availability, fall back to other free sources

### Scraping Discipline
- Rate limiting on all requests
- Cache all downloaded PDFs in `data/raw/`
- Store parsed data in SQLite (`data/db/`)

---

## 5. Regulatory Framework

Classification and eligibility checks reference:

- **CRR2** — Articles 72a-72c (eligible liabilities definition)
- **BRRD2** — Articles 45b-45c (MREL calibration)
- **SRB MREL Policy** (2024 edition) — operational guidance
- **Bank of Italy transposition** — national specifics

---

## 6. Dashboard Design

### View 1: Instrument Explorer
- Filterable/sortable table of all instruments
- Columns: ISIN, name, category, issue date, maturity, residual maturity, coupon type, outstanding amount, MREL eligible (Y/N), eligibility reason
- Filters: category, eligibility, maturity bucket, listing venue
- Row click → full detail (prospectus clauses, parsed terms)

### View 2: MREL Stack Waterfall
- Horizontal waterfall: CET1 → AT1 → T2 → SNP → Senior Preferred → Structured Notes → Total MREL
- Overlaid requirement lines: subordination requirement, total MREL requirement, CBR
- Shows surplus/shortfall at each level

### View 3: Reconciliation
- Side-by-side: bottom-up totals vs Pillar 3 aggregates
- Delta column highlighting discrepancies
- Drill-down into unmatched instruments

### View 4: Data Quality & Audit
- Instruments with uncertain classification (ambiguous prospectus language)
- Missing data flags (no outstanding amount, no prospectus matched)
- PDF parsing confidence scores
- Export full dataset to Excel

### Cross-cutting Features
- Reference date selector (default 31.12.2024)
- Excel export on every view
- Source links (original prospectus PDF, Borsa Italiana page)

---

## 7. Tech Stack

- **Python 3.14**
- **Streamlit** — dashboard framework
- **httpx** — async HTTP client for scraping
- **BeautifulSoup** — HTML parsing
- **PyMuPDF (fitz)** or **pdfplumber** — PDF text extraction
- **pandas** — data processing
- **plotly** — interactive charts
- **SQLite** — local data storage
- **python-dotenv** — environment config
