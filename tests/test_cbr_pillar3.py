from __future__ import annotations

import pandas as pd

from scrapers.cbr_pillar3 import (
    choose_best_pdf_link,
    classify_cbr_text,
    extract_pdf_links,
    filter_banks_from_long_df,
    list_italian_banks_for_reference_date,
)


def test_filter_banks_from_long_df_returns_italian_june_2025_banks() -> None:
    records_df = pd.DataFrame(
        [
            {"entity_name": "Banco BPM", "country": "Italy", "reference_date": "2025-06-30"},
            {"entity_name": "Mediobanca", "country": "Italy", "reference_date": "2025-06-30"},
            {"entity_name": "Banco BPM", "country": "Italy", "reference_date": "2025-12-31"},
            {"entity_name": "ABN AMRO", "country": "Netherlands", "reference_date": "2025-06-30"},
        ]
    )

    assert filter_banks_from_long_df(records_df) == ["Banco BPM", "Mediobanca"]


def test_extract_pdf_links_and_choose_best_pdf_link() -> None:
    html = """
    <html>
      <body>
        <a href="/docs/pillar-marzo-2025.pdf">Pillar 3 - marzo 2025</a>
        <a href="/docs/pillar-giugno-2025.pdf">Informativa al pubblico - giugno 2025</a>
        <a href="/docs/bilancio-2025.pdf">Bilancio</a>
      </body>
    </html>
    """

    links = extract_pdf_links(html, "https://example.com/investors/pillar-3")
    best = choose_best_pdf_link(links)

    assert best == "https://example.com/docs/pillar-giugno-2025.pdf"


def test_classify_cbr_text_detects_explicit_on_top() -> None:
    pages = [
        "The MREL requirement is applicable in addition to the combined buffer requirement.",
    ]

    assert classify_cbr_text(pages) == "explicit_on_top"


def test_classify_cbr_text_detects_unclear_mention() -> None:
    pages = [
        "The MREL section states that the combined buffer requirement is disclosed in the section below.",
    ]

    assert classify_cbr_text(pages) == "mentioned_unclear"


def test_classify_cbr_text_detects_italian_on_top_wording() -> None:
    pages = [
        "Il requisito MREL-TREA è pari al 19,55% maggiorato del Combined Buffer Requirement applicabile.",
    ]

    assert classify_cbr_text(pages) == "explicit_on_top"


def test_classify_cbr_text_detects_incl_cbr_wording() -> None:
    pages = [
        "MREL-TREA: 23,92% (incl. CBR) con un subordination requirement del 16,67%",
    ]

    assert classify_cbr_text(pages) == "explicit_included"


def test_classify_cbr_text_ignores_non_mrel_cbr_mentions() -> None:
    pages = [
        "Il Tier 1 Ratio è costituito dalla somma del requisito minimo regolamentare e del Combined Buffer Requirement.",
    ]

    assert classify_cbr_text(pages) == "no_match"


def test_classify_cbr_text_detects_banco_bpm_added_wording() -> None:
    pages = [
        "Il requisito minimo di MREL-TREA sopra riportato va aggiunto il requisito combinato di riserva di capitale applicabile al Gruppo Banco BPM.",
        "I requisiti MREL-TREA sono riportati al netto del requisito combinato di riserva di capitale, che il Gruppo è tenuto a rispettare in aggiunta ai requisiti MREL-TREA.",
    ]

    assert classify_cbr_text(pages) == "explicit_on_top"


def test_list_italian_banks_for_reference_date_adds_manual_extra_banks(monkeypatch) -> None:
    import pandas as pd

    def fake_loader(_workbook_path=None):
        return pd.DataFrame(
            [
                {"entity_name": "BANCO BPM SOCIETA' PER AZIONI", "country": "Italy", "reference_date": "2025-06-30"},
            ]
        )

    monkeypatch.setattr("scrapers.cbr_pillar3.load_official_pillar3_long", fake_loader)

    banks = list_italian_banks_for_reference_date("2025-06-30")

    assert "BANCO BPM SOCIETA' PER AZIONI" in banks
    assert "UniCredit S.p.A." in banks
    assert "Intesa Sanpaolo S.p.A." in banks
    assert "BPER Banca S.p.A." in banks
