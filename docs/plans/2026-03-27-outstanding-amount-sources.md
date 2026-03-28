# Outstanding Amount Sources for Banco BPM Instruments

**Date:** 2026-03-27
**Context:** Research on data sources for obtaining outstanding amounts (importo in circolazione) for 189 Banco BPM ISINs including bonds, structured notes, certificates, and covered bonds.

---

## Executive Summary

**Best source found: ESMA FIRDS Solr API** -- free, programmatic, returns `bnd_nmnl_value_total` (total issued nominal amount) for all instrument types. Confirmed working for Banco BPM bonds, certificates, and structured notes. 26,192 records exist for Banco BPM's LEI across all trading venues.

**Important caveat:** FIRDS provides the **total issued nominal amount** (original issuance size), NOT the current outstanding amount after any buybacks or amortizations. For most Banco BPM instruments these should be identical, but for amortizing covered bonds or partially redeemed instruments, additional data may be needed.

---

## Source-by-Source Analysis

### 1. ESMA FIRDS (Financial Instruments Reference Data System) -- BEST SOURCE

| Attribute | Detail |
|-----------|--------|
| **Outstanding data available?** | YES -- `bnd_nmnl_value_total` (total issued nominal amount) |
| **Free or paid?** | Free, public |
| **Programmatic access?** | YES -- Solr API, JSON/XML responses |
| **Reliability** | High -- regulatory data submitted by trading venues under MiFIR |
| **Coverage** | Excellent for listed/traded instruments; 26,192 records for Banco BPM LEI |
| **Access restrictions** | None -- public API, no authentication required |

**API Endpoint:**
```
https://registers.esma.europa.eu/solr/esma_registers_firds/select
```

**Query by ISIN (single instrument):**
```
?q=isin:IT0005580136&wt=json&rows=1&fl=isin,gnr_full_name,bnd_nmnl_value_total,bnd_nmnl_value_unit,bnd_nmnl_value_curr_code,bnd_maturity_date,bnd_fixed_rate,bnd_seniority,lei,gnr_cfi_code
```

**Query by LEI (all Banco BPM instruments):**
```
?q=lei:815600E4E6DCD2D25E30&wt=json&rows=100&fl=isin,gnr_full_name,bnd_nmnl_value_total,bnd_nmnl_value_curr_code,bnd_seniority,bnd_maturity_date,gnr_cfi_code
```

**Key data fields returned:**
- `isin` -- ISIN code
- `gnr_full_name` -- instrument name
- `gnr_cfi_code` -- CFI classification code (D=Debt, DS=Structured, DT=Bonds, DB=subordinated)
- `bnd_nmnl_value_total` -- **total issued nominal amount** (e.g., "750000000")
- `bnd_nmnl_value_unit` -- denomination per unit (e.g., "150000" or "1000")
- `bnd_nmnl_value_curr_code` -- currency (e.g., "EUR")
- `bnd_maturity_date` -- maturity date
- `bnd_fixed_rate` -- fixed coupon rate
- `bnd_seniority` -- seniority (SNDB=Senior, JUND=Junior/Subordinated, MZZD=Mezzanine)
- `lei` -- issuer LEI

**Verified examples:**
| ISIN | Name | Total Issued | Type |
|------|------|-------------|------|
| IT0005580136 | BAMIIM 4 7/8 01/17/30 | 750,000,000 EUR | Senior bond |
| XS2229021261 | BcBPM 5% 14/09/30 | 500,000,000 EUR | T2 bond |
| IT0005651788 | Banco BPM 6.25% perp | 400,000,000 EUR | AT1 (JUND) |
| IT0005427866 | BPM EP CAP DIVDAX | 155,232,500 EUR | Certificate (DSDVFI) |
| XS2577572188 | BcBPM 4.875% 18/01/27 | 750,000,000 EUR | Senior bond (SNDB) |

**Note:** Multiple records per ISIN (one per trading venue). Use `group=true&group.field=isin` or deduplicate in code.

**Official Python package:** `esma_data_py` (https://github.com/European-Securities-Markets-Authority/esma_data_py) -- for bulk XML file downloads.

**Bulk file download URL pattern:**
```
https://firds.esma.europa.eu/firds/FULINS_S_YYYYMMDD_NNofNN.zip
```
Files split by CFI code first letter. Full files published weekly, delta files daily by 09:00 CET.

---

### 2. Borsa Italiana Detail Pages

| Attribute | Detail |
|-----------|--------|
| **Outstanding data available?** | PARTIAL -- "Outstanding" field shown on MOT government bond pages; NOT shown on EuroTLX corporate bond pages |
| **Free or paid?** | Free (website) |
| **Programmatic access?** | Scraping only -- no public API; pages render via JavaScript |
| **Reliability** | High for government bonds; inconsistent for corporate bonds |
| **Coverage** | Only bonds listed on MOT/EuroTLX |
| **Access restrictions** | Terms of use may prohibit scraping |

**Findings:**
- The "dati-completi" (complete data) page for a French OAT government bond on MOT shows an **"Outstanding: 37,723,000,000"** field.
- The same page type for Banco BPM corporate bonds on EuroTLX does NOT show an "Outstanding" field.
- Fields shown for EuroTLX corporate bonds: ISIN, issuer, coupon rate, maturity, lot size (150,000), prices, yields, accrued interest, modified duration. No outstanding amount.
- Borsa Italiana pages are heavily JavaScript-rendered; direct URL fetch returns search forms, not data.

**URL patterns:**
```
# MOT government bonds (has "Outstanding"):
/borsa/obbligazioni/mot/euro-obbligazioni/scheda/{ISIN}-MOTX.html

# EuroTLX corporate bonds (NO "Outstanding"):
/borsa/obbligazioni/eurotlx/scheda/{ISIN}.html
/borsa/obbligazioni/eurotlx/dati-completi.html?isin={ISIN}
```

**Verdict:** Not useful as primary source for Banco BPM instrument outstanding amounts. The "Outstanding" field appears to be available only for government bonds on MOT.

---

### 3. ECB Centralised Securities Database (CSDB)

| Attribute | Detail |
|-----------|--------|
| **Outstanding data available?** | NO -- not at individual security level |
| **Free or paid?** | N/A -- not publicly accessible |
| **Programmatic access?** | No |
| **Reliability** | N/A |
| **Coverage** | N/A |
| **Access restrictions** | Restricted to ESCB members (central banks) |

**Findings:**
- The CSDB is jointly operated by ESCB members and is NOT accessible to the public.
- Publicly available data from CSDB is through the CSEC (Securities Issues Statistics) dataset on the ECB Data Portal, which provides AGGREGATE statistics only -- breakdowns by sector, maturity, coupon type, and currency. No individual security data.
- CSEC URL: https://data.ecb.europa.eu/data/datasets/csec/data-information
- Data released with t+10 working days timeliness.

**Verdict:** Not usable for individual instrument outstanding amounts.

---

### 4. Banco BPM Pillar 3 / CRR Art. 437 Disclosure

| Attribute | Detail |
|-----------|--------|
| **Outstanding data available?** | YES but LIMITED to 12 capital instruments (CET1 + AT1 + T2) |
| **Free or paid?** | Free |
| **Programmatic access?** | Excel download for Annex 1; PDF for full report |
| **Reliability** | Very high -- regulatory disclosure |
| **Coverage** | Only CET1, AT1, and T2 instruments. Does NOT cover Senior Preferred, Senior Non-Preferred, or Covered Bonds |
| **Access restrictions** | None |

**Available documents (as of Dec 2024):**
- Pillar 3 report (PDF, 348 pages): https://gruppo.bancobpm.it/media/dlm_uploads/Pillar-3-December-2024_Doc_EN.pdf
- Annex 1 Equity Instruments (XLSX): https://gruppo.bancobpm.it/media/dlm_uploads/Annex-1_Equity-Instruments_12_2024_EN.xlsx

**EU CCA template (from Annex 1 XLSX) contains 12 instruments:**

| ISIN | Type | Nominal (EUR M) | Maturity |
|------|------|---------------:|----------|
| IT0005218380 | CET1 (ordinary shares) | N/A | Perpetual |
| XS2089968270 | AT1 | 400 | Perpetual |
| XS2284323347 | AT1 | 400 | Perpetual |
| XS2398286471 | AT1 | 300 | Perpetual |
| IT0005571309 | AT1 | 300 | Perpetual |
| IT0005604803 | AT1 | 400 | Perpetual |
| XS2229021261 | T2 | 500 | 2030-09-14 |
| XS2271367315 | T2 | 350 | 2031-01-14 |
| XS2358835036 | T2 | 300 | 2031-06-29 |
| XS2434421413 | T2 | 400 | 2032-01-19 |
| IT0005586729 | T2 | 500 | 2034-06-18 |
| IT0005623837 | T2 | 500 | 2036-11-26 |

**Key fields per instrument:** Issuer, ISIN, public/private placement, governing law, regulatory treatment, instrument type, nominal amount (row 9), issue price, redemption price, maturity, call features, coupon details, seniority ranking, subordination type, write-down mechanics, link to full terms.

**Pillar 3 also reports quarterly:** Pillar III data as at 31.03.2025, 30.06.2024, 30.09.2024, 31.12.2024.

**Verdict:** Essential for capital instruments. Provides authoritative nominal amounts for AT1 and T2. Does not cover senior bonds or certificates, which represent the bulk of the 189 ISINs.

---

### 5. Banca d'Italia Statistical Databases

| Attribute | Detail |
|-----------|--------|
| **Outstanding data available?** | NO at individual security level for corporate bonds |
| **Free or paid?** | Free for aggregates; restricted for individual security data |
| **Programmatic access?** | InfoStat platform (https://infostat.bancaditalia.it/inquiry/) |
| **Reliability** | Very high |
| **Coverage** | Aggregate bank bond statistics only |
| **Access restrictions** | Anagrafe Titoli (individual security data) restricted to regulated entities |

**Findings:**
- **Base Dati Statistica (BDS):** Public aggregate statistics on bank bond issuances. Breakdowns by bank sector, maturity, currency. No individual ISIN-level data.
- **Anagrafe Titoli (Securities Database):** Contains individual security data including outstanding amounts, but access is restricted to: central authorities, National Numbering Agencies, public administrations, and supervised entities (banks, SIMs, etc.). Anyone else can request access by paying a fee, but only for own operational use.
- **ISIN coding service:** Banca d'Italia is the Italian NNA (National Numbering Agency) and assigns ISIN codes. The FEAT (Front End Anagrafe Titoli) online procedure is available on the INFOSTAT platform to accredited entities.

**Verdict:** Not practically accessible for our purposes. The Anagrafe Titoli would be ideal but is restricted.

---

### 6. Cbonds.com

| Attribute | Detail |
|-----------|--------|
| **Outstanding data available?** | YES -- "Outstanding Amount" field available per bond |
| **Free or paid?** | PAID -- subscription from $350/month; API requires bilateral agreement with legal entity |
| **Programmatic access?** | REST API, SOAP API, SFTP, email delivery; JSON/XML/CSV/XLS formats |
| **Reliability** | High -- professional data provider, 100+ field types |
| **Coverage** | Good for institutional bonds; coverage of Italian retail certificates/structured notes uncertain |
| **Access restrictions** | Subscription required; API access needs bilateral legal agreement |

**Findings:**
- Cbonds has individual bond pages for Banco BPM (e.g., IT0005580136, XS2398286471)
- Pages show: placement amount, outstanding amount, outstanding face value, USD equivalents
- API has 100+ field types including: ISIN, ticker, region/sector/industry, security type, interest rate type, coupon, day counting rules, **outstanding/nominal/principal amounts**
- Free testing period available on a few instruments
- Website access returns 403 for scraping

**Verified examples visible in search results:**
- IT0005580136: Placement 750,000,000 EUR, Outstanding 750,000,000 EUR
- XS2398286471: Placement 300,000,000 EUR, Outstanding 300,000,000 EUR

**Verdict:** Good data quality but cost-prohibitive for this project. Would be useful for cross-validation.

---

### 7. Investing.com

| Attribute | Detail |
|-----------|--------|
| **Outstanding data available?** | UNCERTAIN -- bond detail pages exist but specific field availability unconfirmed |
| **Free or paid?** | Free (website) |
| **Programmatic access?** | No official API for bond data; scraping possible but against ToS |
| **Reliability** | Medium |
| **Coverage** | Likely limited for Italian domestic retail bonds/certificates |
| **Access restrictions** | ToS prohibit scraping |

**Findings:**
- Investing.com has some Banco BPM bond news coverage (e.g., senior non-preferred issuances)
- Bond detail pages are difficult to find by ISIN -- URL structure not ISIN-based
- Search for specific Banco BPM ISINs returned no direct investing.com bond detail pages
- The platform focuses more on government bonds and major corporate issues

**Verdict:** Not a practical source for systematic data collection.

---

### 8. OpenFIGI API

| Attribute | Detail |
|-----------|--------|
| **Outstanding data available?** | NO |
| **Free or paid?** | Free (with rate limits) |
| **Programmatic access?** | REST API |
| **Reliability** | N/A for amounts |
| **Coverage** | N/A |
| **Access restrictions** | Rate-limited; higher limits with API key |

**Findings:**
- The OpenFIGI API maps identifiers (ISIN -> FIGI) and returns classification metadata only
- Response fields: figi, securityType, marketSector, ticker, name, exchCode, compositeFIGI, shareClassFIGI, securityType2, securityDescription
- Does NOT return: outstanding amount, notional amount, issue size, face value, or any financial amounts
- Useful for identifier mapping, not for financial data

**Verdict:** Not useful for outstanding amounts. Could be useful for identifier cross-referencing only.

---

### 9. Banco BPM Investor Relations

| Attribute | Detail |
|-----------|--------|
| **Outstanding data available?** | PARTIAL -- no single consolidated list with outstanding amounts |
| **Free or paid?** | Free |
| **Programmatic access?** | PDF/XLSX downloads; no API |
| **Reliability** | Very high -- issuer data |
| **Coverage** | Comprehensive for instrument documentation, but amounts require parsing Final Terms PDFs |
| **Access restrictions** | None (domestic issues documentation in Italian only) |

**Key pages:**
- Investor Relations: https://gruppo.bancobpm.it/en/investor-relations/
- Debt Instruments: https://gruppo.bancobpm.it/en/investor-relations/financial-instruments/
- International Issues: https://gruppo.bancobpm.it/en/investor-relations/debt-instruments/international-issues/
- Domestic Issues: https://gruppo.bancobpm.it/investor-relations/strumenti-di-debito/emissioni-domestiche/
- Pillar 3: https://gruppo.bancobpm.it/en/investor-relations/pillar-3/

**Available data:**
- EMTN Programme final terms (PDFs) -- contain nominal amounts per series
- Covered Bond Programme documentation
- Certificates documentation with ISINs
- Quarterly Group Profile presentations with MREL buffer calculations (aggregate)
- Annual Report with liability structure

**Domestic issues page lists:**
- Zero Coupon, Fixed Rate, Variable Rate, Structured bonds
- Capital Protected and Conditionally Protected certificates
- Individual ISINs visible (e.g., IT0005692246, IT0005695678, IT0005697989)
- Actual amounts only in linked PDF Final Terms documents

**Verdict:** Authoritative for instrument classification but requires PDF parsing for amounts. Already integrated in the project's pipeline. Not a practical source for bulk outstanding amounts.

---

### 10. Monte Titoli (Italian CSD)

| Attribute | Detail |
|-----------|--------|
| **Outstanding data available?** | NO -- not publicly accessible at individual security level |
| **Free or paid?** | N/A |
| **Programmatic access?** | No |
| **Reliability** | N/A |
| **Coverage** | N/A |
| **Access restrictions** | Data only available to CSD participants |

**Findings:**
- Monte Titoli is Italy's CSD, now part of Euronext Securities Milan
- Handles central safekeeping and administration for Italian private-sector bonds and certificates
- Some aggregate data available through ECB Statistical Data Warehouse (securities held statistics)
- No public API or data download for individual security outstanding amounts
- CSD participants (banks, brokers) can access settlement and holding data

**Verdict:** Not usable. Monte Titoli data is only accessible to market participants.

---

## Recommended Data Strategy

### Primary Source: ESMA FIRDS API

1. **Query by ISIN** for each of the 189 instruments
2. Extract `bnd_nmnl_value_total` as the issued nominal amount
3. Also extract: `gnr_cfi_code` (for classification), `bnd_seniority`, `bnd_maturity_date`, `bnd_fixed_rate`

**Implementation approach:**
```python
import httpx

FIRDS_URL = "https://registers.esma.europa.eu/solr/esma_registers_firds/select"

async def get_firds_data(isin: str) -> dict:
    params = {
        "q": f"isin:{isin}",
        "wt": "json",
        "rows": 1,
        "fl": "isin,gnr_full_name,gnr_cfi_code,bnd_nmnl_value_total,bnd_nmnl_value_unit,bnd_nmnl_value_curr_code,bnd_maturity_date,bnd_fixed_rate,bnd_seniority,lei"
    }
    async with httpx.AsyncClient() as client:
        resp = await client.get(FIRDS_URL, params=params)
        data = resp.json()
        if data["response"]["numFound"] > 0:
            return data["response"]["docs"][0]
    return None
```

### Secondary Source: Banco BPM Pillar 3 EU CCA Template

- Already downloaded: `data/raw/pillar3/EU_CCA_20241231.xlsx`
- Provides authoritative nominal amounts for 12 capital instruments (CET1, AT1, T2)
- Use as ground truth for these instruments; cross-validate against FIRDS

### Cross-validation

- For the 12 EU CCA instruments, compare Pillar 3 amounts vs. FIRDS amounts
- Any discrepancies may indicate buybacks, taps, or amortizations
- FIRDS amount should equal or exceed Pillar 3 amount (Pillar 3 may reflect regulatory deductions)

### Coverage Gap

FIRDS provides **total issued nominal amount** (amount at issuance). This differs from **current outstanding** when:
- The issuer has bought back bonds
- Covered bonds have amortized
- Partial redemptions have occurred

For MREL analysis purposes, the issued nominal amount is a reasonable proxy since:
- Most Banco BPM bonds are bullet (no amortization)
- Significant buybacks would be disclosed in financial statements
- The Pillar 3 aggregate reconciliation will catch material discrepancies

---

## Summary Table

| # | Source | Outstanding Data | Free | API | Best Use |
|---|--------|-----------------|------|-----|----------|
| 1 | **ESMA FIRDS** | YES (issued nominal) | Yes | Yes (Solr) | **PRIMARY: bulk data for all 189 ISINs** |
| 2 | Borsa Italiana | Partial (govt only) | Yes | No (scraping) | Not recommended |
| 3 | ECB CSDB | No (aggregates only) | N/A | N/A | Not usable |
| 4 | **Banco BPM Pillar 3** | YES (12 instruments) | Yes | XLSX | **SECONDARY: AT1/T2 ground truth** |
| 5 | Banca d'Italia | No (restricted) | Restricted | Restricted | Not practically accessible |
| 6 | Cbonds.com | YES | Paid ($350/mo) | Yes (REST) | Cross-validation (if budget allows) |
| 7 | Investing.com | Uncertain | Free | No | Not practical |
| 8 | OpenFIGI | NO | Free | Yes (REST) | Identifier mapping only |
| 9 | Banco BPM IR | Partial (PDFs) | Free | No | Classification & documentation |
| 10 | Monte Titoli | No (restricted) | N/A | No | Not usable |
