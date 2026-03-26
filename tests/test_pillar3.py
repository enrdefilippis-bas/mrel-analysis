from scrapers.pillar3 import _parse_number, Pillar3Aggregates, _extract_values_from_table

def test_parse_number_italian():
    assert _parse_number("1.234.567,89") == 1234567.89
    assert _parse_number("500.000") == 500000.0
    assert _parse_number("-") is None
    assert _parse_number("n.a.") is None

def test_extract_values_from_table():
    table = [
        ["CET1 capital", "10.500"],
        ["Additional Tier 1", "1.200"],
        ["Tier 2", "2.300"],
        ["Fondi Propri", "14.000"],
    ]
    agg = Pillar3Aggregates()
    _extract_values_from_table(table, agg)
    assert agg.cet1 == 10500.0
    assert agg.at1 == 1200.0
    assert agg.tier2 == 2300.0
    assert agg.total_own_funds == 14000.0
