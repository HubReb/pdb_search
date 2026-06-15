"""Real-database tests for the bulk-import path.

Verify that every cited entry with a matching ``.bib`` record is imported, that a cited key with
no matching record is skipped (not fatal), and that a rerun does not duplicate (BibTeX-key
uniqueness).
"""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import Engine

from paper_sorts.cli.importer import run_import
from paper_sorts.services.import_service import extract_papers_from_tex_bib
from paper_sorts.services.paper_service import PaperService

_FIXTURES = Path(__file__).parent / "fixtures"
_TEX = _FIXTURES / "sample.tex"
_BIB = _FIXTURES / "sample.bib"


def test_extract_skips_unmatched_key() -> None:
    """The extractor yields only cited keys with a matching bib entry."""
    papers = list(extract_papers_from_tex_bib(_TEX, _BIB))
    keys = {p.bibtex_id for p in papers}
    assert keys == {"Alpha2020", "Beta2021"}
    assert "Gamma2099" not in keys


def test_extract_parses_authors() -> None:
    """Authors are normalised to 'Last, First'."""
    papers = {p.bibtex_id: p for p in extract_papers_from_tex_bib(_TEX, _BIB)}
    assert "Adams, Alice" in papers["Alpha2020"].authors
    assert "Brown, Bob" in papers["Alpha2020"].authors


def test_import_inserts_matched_entries(engine: Engine) -> None:
    """Importing inserts the matched papers and they are retrievable."""
    service = PaperService(engine)
    inserted = run_import(service, _TEX, _BIB)
    assert inserted == 2
    assert len(service.search_by_author("Adams, Alice")) == 1
    assert len(service.search_by_title("Beta: a second paper")) == 1


def test_import_rerun_does_not_duplicate(engine: Engine) -> None:
    """A second import of the same pair inserts nothing new (key uniqueness)."""
    service = PaperService(engine)
    assert run_import(service, _TEX, _BIB) == 2
    assert run_import(service, _TEX, _BIB) == 0
    assert len(service.search_by_title("Alpha: a first paper")) == 1
