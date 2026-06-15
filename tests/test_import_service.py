"""Tests for the bulk-import extraction and the import command.

The fixture pair ``tests/fixtures/literature_overview.tex`` +
``tests/fixtures/bib.bib`` defines three cited papers, one of whose keys
(``missingkey``) has no matching BibTeX record and must be skipped.
"""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import Engine

from paper_sorts.cli.importer import run_import
from paper_sorts.services.import_service import extract_papers_from_tex_bib
from paper_sorts.services.paper_service import PaperService

FIXTURES = Path(__file__).parent / "fixtures"
TEX = str(FIXTURES / "literature_overview.tex")
BIB = str(FIXTURES / "bib.bib")


def test_extract_skips_unmatched_key() -> None:
    """Only cited keys with a matching .bib record are extracted."""
    papers = list(extract_papers_from_tex_bib(TEX, BIB))
    keys = {p.bibtex_id for p in papers}
    assert keys == {"paperone", "papertwo"}
    assert "missingkey" not in keys


def test_extract_authors_and_titles() -> None:
    """Extracted papers carry their parsed authors and titles."""
    papers = {p.bibtex_id: p for p in extract_papers_from_tex_bib(TEX, BIB)}
    assert papers["paperone"].title == "The First Imported Paper"
    assert papers["paperone"].authors == ["Doe, John", "Roe, Jane"]
    assert papers["papertwo"].authors == ["Stark, Tony"]


def test_import_inserts_and_is_idempotent(engine: Engine) -> None:
    """Importing inserts matched papers; a rerun inserts nothing new."""
    inserted = run_import(engine, TEX, BIB)
    assert inserted == 2
    service = PaperService(engine)
    assert service.search_by_title("The First Imported Paper")[0].authors == [
        "Doe, John",
        "Roe, Jane",
    ]
    # Rerun: already-present keys are skipped (per-paper commit + key uniqueness).
    assert run_import(engine, TEX, BIB) == 0
