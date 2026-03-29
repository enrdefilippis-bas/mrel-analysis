CBR PDF scraping outputs live here.

Expected runtime structure:
- `cbr/raw/` downloaded Pillar 3 PDFs
- `cbr/text/` extracted plain text by page
- `cbr/json/` structured evidence per bank
- `cbr/summary.csv` aggregate scrape summary
- `cbr/dataset.csv` normalized one-row-per-bank dataset for dashboard ingestion
- `cbr/dataset.json` same normalized dataset in JSON format
