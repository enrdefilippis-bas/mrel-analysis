import pytest
from scrapers.banco_bpm import extract_pdf_links, ISIN_PATTERN

SAMPLE_HTML = """
<html><body>
<div>
  <a href="/media/dlm_uploads/IT0005692246_Condizioni_Definitive.pdf">
    IT0005692246 - Condizioni Definitive e Nota di Sintesi
  </a>
  <a href="/media/dlm_uploads/Base_Prospectus_2025.pdf">
    Base Prospectus - 16 May 2025
  </a>
  <a href="/media/dlm_uploads/some_doc.docx">Not a PDF</a>
</div>
</body></html>
"""

def test_extract_pdf_links():
    links = extract_pdf_links(SAMPLE_HTML, "domestic")
    assert len(links) == 2
    ft = [l for l in links if l.doc_type == "final_terms"]
    assert len(ft) == 1
    assert ft[0].isin == "IT0005692246"

def test_isin_pattern():
    assert ISIN_PATTERN.search("IT0005692246") is not None
    assert ISIN_PATTERN.search("XS1686880599") is not None
    assert ISIN_PATTERN.search("US1234567890") is None
    assert ISIN_PATTERN.search("NOTANISIN") is None
