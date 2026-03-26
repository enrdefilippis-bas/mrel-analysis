from parsers.prospectus import parse_prospectus


def test_parse_senior_vanilla():
    text = """
    Codice ISIN: IT0005692246
    Obbligazioni a Tasso Fisso 4.50% 15/01/2027
    Valore Nominale: EUR 1.000
    Data di Emissione: 15/01/2024
    Data di Scadenza: 15/01/2027
    Tasso di Interesse: 4.50% annuo
    Rimborso a scadenza: 100% del Valore Nominale
    Le obbligazioni sono soggette allo strumento del bail-in ai sensi della BRRD.
    """
    data = parse_prospectus(text)
    assert data.isin == "IT0005692246"
    assert data.is_subordinated is False
    assert data.is_underlying_linked is False
    assert data.has_bail_in_clause is True
    assert data.is_capital_protected is True


def test_parse_certificate():
    text = """
    Codice ISIN: IT0005695249
    Certificati Banco BPM Autocallable con Barriera
    Sottostante: Indice FTSE MIB
    Livello Barriera: 60% del valore iniziale
    Rimborso anticipato automatico condizionato
    """
    data = parse_prospectus(text)
    assert data.isin == "IT0005695249"
    assert data.has_autocallable is True
    assert data.has_barrier is True
    assert data.is_underlying_linked is True


def test_parse_snp():
    text = """
    ISIN: XS2034154190
    Senior Non-Preferred Notes
    Art. 12-c del Regolamento CRR
    Soggette a bail-in ai sensi della Direttiva BRRD
    Data di Scadenza: 15/06/2029
    """
    data = parse_prospectus(text)
    assert data.isin == "XS2034154190"
    assert data.is_senior_non_preferred is True
    assert data.has_bail_in_clause is True


def test_parse_tier2():
    text = """
    ISIN: IT0005572166
    Obbligazioni Subordinate Tier 2 a Tasso Fisso
    Classe subordinata
    Data di Scadenza: 20/03/2034
    Soggette a write-down e conversione ai sensi della BRRD
    """
    data = parse_prospectus(text)
    assert data.isin == "IT0005572166"
    assert data.is_subordinated is True
    assert data.has_bail_in_clause is True


def test_parse_structured_note_protected():
    text = """
    ISIN: IT0005697989
    Obbligazioni con Opzione Digitale legate all'indice Euribor 3M
    Sottostante: Euribor 3M
    Protezione del capitale a scadenza
    Rimborso minimo a scadenza pari al 100% del Valore Nominale
    """
    data = parse_prospectus(text)
    assert data.isin == "IT0005697989"
    assert data.is_underlying_linked is True
    assert data.is_capital_protected is True
    assert data.has_barrier is False
