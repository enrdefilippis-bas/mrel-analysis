"""Find all missing institutional bonds (AT1, T2, Senior) for Banco BPM."""
import requests
import sqlite3

LEI = "815600E4E6DCD2D25E30"
FIRDS_URL = "https://registers.esma.europa.eu/solr/esma_registers_firds/select"

def query_esma(q, rows=500):
    r = requests.get(FIRDS_URL, params={
        'q': q, 'wt': 'json', 'rows': rows,
        'fl': 'isin,gnr_full_name,bnd_nmnl_value_total,bnd_maturity_date,bnd_seniority,gnr_cfi_code,bnd_fixed_rate',
    })
    return r.json().get('response', {}).get('docs', [])

def query_isin(isin):
    docs = query_esma(f'isin:{isin} AND type_s:parent', rows=1)
    return docs[0] if docs else None


conn = sqlite3.connect('data/db/mrel.db')
db_isins = set(r[0] for r in conn.execute('SELECT isin FROM instruments').fetchall())
conn.close()

all_bonds = {}

# 1. AT1 perpetuals (JUND seniority)
for doc in query_esma(f'lei:{LEI} AND type_s:parent AND bnd_seniority:JUND'):
    all_bonds.setdefault(doc['isin'], doc)

# 2. Tier 2 (SBOD seniority)
for doc in query_esma(f'lei:{LEI} AND type_s:parent AND bnd_seniority:SBOD'):
    all_bonds.setdefault(doc['isin'], doc)

# 3. Senior bonds with XS prefix (international = institutional EMTN)
for doc in query_esma(f'lei:{LEI} AND type_s:parent AND isin:XS* AND bnd_seniority:SNDB'):
    all_bonds.setdefault(doc['isin'], doc)

# 4. Directly look up known institutional ISINs
known_isins = [
    # From Pillar 3 EU CCA
    'XS2089968270', 'XS2284323347', 'XS2398286471', 'IT0005571309', 'IT0005604803',
    'XS2229021261', 'XS2271367315', 'XS2358835036', 'XS2434421413', 'IT0005586729', 'IT0005623837',
    # Known EMTN senior bonds
    'XS2577572188', 'XS2121417989', 'XS2034154190', 'XS2063556117',
    'XS2530053789', 'XS2558591967', 'XS1686880599', 'XS1984319316',
    # Known IT-prefix institutional bonds (from TradingView data)
    'IT0005549479', 'IT0005580136', 'IT0005611253', 'IT0005696668',
    'IT0005632267', 'IT0005675126', 'IT0005657850', 'IT0005651788',
    # SNP bonds already in DB
    'IT0005640203', 'IT0005641540',
]
for isin in known_isins:
    if isin not in all_bonds:
        doc = query_isin(isin)
        if doc:
            all_bonds[isin] = doc

# Filter and display
print(f"Total unique ISINs: {len(all_bonds)}\n")
print(f"{'DB':2s}  {'ISIN':15s}  {'AMT':>10s}  {'SEN':5s}  {'RATE':>6s}  {'MAT':10s}  {'CFI':7s}  NAME")
print("-" * 110)

active = []
for isin, doc in all_bonds.items():
    mat = (doc.get('bnd_maturity_date') or '')[:10]
    if mat and mat < '2024-12-31' and not mat.startswith('9999'):
        continue
    amt = float(doc.get('bnd_nmnl_value_total', '0'))
    if amt < 50_000_000:
        continue
    active.append((isin, doc, amt))

active.sort(key=lambda x: -x[2])
for isin, doc, amt in active:
    mat = (doc.get('bnd_maturity_date') or '')[:10]
    sen = doc.get('bnd_seniority') or 'N/A'
    cfi = doc.get('gnr_cfi_code') or ''
    is_cb = len(cfi) >= 4 and cfi[3] == 'S'
    status = 'Y' if isin in db_isins else 'N'
    rate = str(doc.get('bnd_fixed_rate') or 'FRN')
    name = (doc.get('gnr_full_name') or '')[:48]
    cb = ' [CB]' if is_cb else ''
    print(f" {status}  {isin}  {amt/1e6:>8.0f}M  {sen:5s}  {rate:>6s}  {mat}  {cfi:7s}  {name}{cb}")

# Summary
in_db = sum(1 for i, _, _ in active if i in db_isins)
missing = sum(1 for i, _, _ in active if i not in db_isins)
print(f"\nIn DB: {in_db}, Missing: {missing}")
