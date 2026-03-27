from scrapers.banco_bpm import extract_pdf_links, ISIN_PATTERN

# Simulates the JSON-embedded structure found on Banco BPM IR pages
SAMPLE_HTML = """
<html><body>
<script>
var documents = [{"title":"IT0005692246 - Condizioni Definitive e Nota di Sintesi","year":"2024","fileUrl":"https://gruppo.bancobpm.it/media/dlm_uploads/IT0005692246_Condizioni_Definitive.pdf","tax":["Prestiti Obbligazionari"],"documentType":"document"},{"title":"Base Prospectus - 16 May 2025","year":"2025","fileUrl":"https://gruppo.bancobpm.it/media/dlm_uploads/Base_Prospectus_2025.pdf","tax":[],"documentType":"document"},{"title":"IT0005602849 - Documenti Emissione","year":"2024","fileUrl":"https://gruppo.bancobpm.it/media/dlm_uploads/IT0005602849_Documenti-Emissione.pdf","tax":["Prestiti Obbligazionari"],"documentType":"document"}];
</script>
</body></html>
"""

def test_extract_pdf_links():
    links = extract_pdf_links(SAMPLE_HTML, "domestic")
    assert len(links) == 3
    ft = [l for l in links if l.doc_type == "final_terms"]
    assert len(ft) == 1
    assert ft[0].isin == "IT0005692246"
    ed = [l for l in links if l.doc_type == "emission_docs"]
    assert len(ed) == 1
    assert ed[0].isin == "IT0005602849"
    bp = [l for l in links if l.doc_type == "base_prospectus"]
    assert len(bp) == 1

def test_isin_pattern():
    assert ISIN_PATTERN.search("IT0005692246") is not None
    assert ISIN_PATTERN.search("XS1686880599") is not None
    assert ISIN_PATTERN.search("US1234567890") is None
    assert ISIN_PATTERN.search("NOTANISIN") is None
