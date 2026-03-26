from scrapers.borsa_italiana import _parse_search_results

SAMPLE_HTML = """
<table>
<tr><th>Nome</th><th>Mercato</th><th>Importo</th></tr>
<tr>
  <td><a href="/borsa/obbligazioni/mot/scheda/IT0005692246.html">BANCO BPM 4.5% 2027</a></td>
  <td>MOT</td>
  <td>500.000.000</td>
</tr>
<tr>
  <td><a href="/borsa/obbligazioni/mot/scheda/XS2034154190.html">BANCO BPM SNP 2029</a></td>
  <td>MOT</td>
  <td>750.000.000</td>
</tr>
</table>
"""

def test_parse_search_results():
    results = _parse_search_results(SAMPLE_HTML)
    assert len(results) == 2
    assert results[0].isin == "IT0005692246"
    assert results[0].outstanding_amount == 500000000.0
    assert results[1].isin == "XS2034154190"
