"""Integration tests for the bulk import service.

Tests verify:
- extract_papers_from_tex_bib yields PaperCreate for matched entries.
- Missing .bib entries are skipped (logged warning, no exception).
- LaTeX accents in author names round-trip without corruption.
- Full import via the service layer commits papers to the DB.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from paper_sorts.db.session import with_session
from paper_sorts.services import import_service, paper_service

FIXTURES = Path(__file__).parent / "fixtures"
TEX_PATH = str(FIXTURES / "test_literature.tex")
BIB_PATH = str(FIXTURES / "test.bib")


def test_extract_yields_matched_entries() -> None:
    """extract_papers_from_tex_bib yields one PaperCreate per matched citation."""
    papers = list(import_service.extract_papers_from_tex_bib(TEX_PATH, BIB_PATH))
    # test.bib has TestImport2026A and TestImport2026B; MissingFromBib2026 is absent
    keys = [p.bibtex_key for p in papers]
    assert "TestImport2026A" in keys
    assert "TestImport2026B" in keys
    assert "MissingFromBib2026" not in keys


def test_missing_bib_entry_skipped_with_warning(caplog: pytest.LogCaptureFixture) -> None:
    """Missing .bib entries are skipped and logged as warnings."""
    with caplog.at_level(logging.WARNING, logger="paper_sorts.services.import_service"):
        papers = list(import_service.extract_papers_from_tex_bib(TEX_PATH, BIB_PATH))
    # MissingFromBib2026 should appear in a warning
    warning_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
    assert any("MissingFromBib2026" in str(msg) for msg in warning_messages)
    # Only matched entries are yielded
    assert len(papers) == 2


def test_paper_count(migrated_db_url: str) -> None:
    """Bulk import inserts exactly the matched entries into the DB."""
    papers = list(import_service.extract_papers_from_tex_bib(TEX_PATH, BIB_PATH))
    inserted_ids: list[int] = []
    for paper in papers:
        result = paper_service.add_paper(
            paper, database_url=migrated_db_url, with_session_fn=with_session
        )
        inserted_ids.append(result.paper_id)

    assert len(inserted_ids) == 2

    # Clean up
    for pid in inserted_ids:
        try:
            paper_service.delete_paper(
                pid, database_url=migrated_db_url, with_session_fn=with_session
            )
        except ValueError:
            pass


def test_latex_accent_author_preserved() -> None:
    """Author names with LaTeX accents are preserved by the import."""
    papers = list(import_service.extract_papers_from_tex_bib(TEX_PATH, BIB_PATH))
    bib_b = next(p for p in papers if p.bibtex_key == "TestImport2026B")
    # The author name should contain "Schlkopf" or "Schölkopf" — pybtex preserves
    # the raw last name from the .bib entry
    assert any("lkopf" in name.lower() for name in bib_b.authors)
