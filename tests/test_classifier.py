from parsers.prospectus import ProspectusData
from parsers.classifier import classify_instrument, prospectus_to_instrument
from models.instrument import InstrumentCategory

def test_classify_senior_vanilla():
    data = ProspectusData(isin="IT0001", is_underlying_linked=False, is_subordinated=False)
    assert classify_instrument(data) == InstrumentCategory.SENIOR_PREFERRED

def test_classify_certificate():
    data = ProspectusData(
        isin="IT0002", is_underlying_linked=True,
        has_barrier=True, has_autocallable=True, is_capital_protected=False,
    )
    assert classify_instrument(data) == InstrumentCategory.CERTIFICATE

def test_classify_structured_note_protected():
    data = ProspectusData(
        isin="IT0003", is_underlying_linked=True,
        is_capital_protected=True, has_barrier=False,
    )
    assert classify_instrument(data) == InstrumentCategory.STRUCTURED_NOTE_PROTECTED

def test_classify_snp():
    data = ProspectusData(isin="XS0001", is_senior_non_preferred=True)
    assert classify_instrument(data) == InstrumentCategory.SENIOR_NON_PREFERRED

def test_classify_tier2():
    data = ProspectusData(isin="IT0004", is_subordinated=True)
    assert classify_instrument(data) == InstrumentCategory.TIER2

def test_classify_covered_bond():
    data = ProspectusData(isin="IT0005", is_covered_bond=True)
    assert classify_instrument(data) == InstrumentCategory.COVERED_BOND

def test_classify_barrier_with_protection():
    data = ProspectusData(
        isin="IT0006", is_underlying_linked=True,
        has_barrier=True, is_capital_protected=True,
    )
    assert classify_instrument(data) == InstrumentCategory.STRUCTURED_NOTE_PROTECTED

def test_prospectus_to_instrument():
    data = ProspectusData(
        isin="IT0005692246", instrument_name="Banco BPM 4.5% 2027",
        is_underlying_linked=False, is_subordinated=False,
        has_bail_in_clause=True, currency="EUR", confidence=0.9,
    )
    inst = prospectus_to_instrument(data)
    assert inst.isin == "IT0005692246"
    assert inst.category == InstrumentCategory.SENIOR_PREFERRED
    assert inst.crr2_rank == 5
    assert inst.bail_in_clause is True
