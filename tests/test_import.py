"""Integration tests for the bulk import service and CLI.

Tests run against the ephemeral PostgreSQL DB (no mocking — Principle II).
Fixture files: tests/fixtures/literature_overview.tex + tests/fixtures/bib.bib
"""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from paper_sorts.cli.app import app
from paper_sorts.db.repositories import PaperRepository
from paper_sorts.db.session import with_session
from paper_sorts.services import import_service

_FIXTURES = Path(__file__).parent / "fixtures"
_TEX = _FIXTURES / "literature_overview.tex"
_BIB = _FIXTURES / "bib.bib"

runner = CliRunner()


def test_extract_papers_from_tex_bib_all_valid() -> None:
    """extract_papers_from_tex_bib yields one PaperCreate per matched entry."""
    tex = _TEX.read_text()
    bib = _BIB.read_text()
    papers = list(import_service.extract_papers_from_tex_bib(tex, bib))
    # 3 of the 4 entries have matching bib records; MissingEntry2022 is skipped
    assert len(papers) == 3
    bibtex_ids = {p.bibtex_id for p in papers}
    assert "Smith2020NLP" in bibtex_ids
    assert "Jones2019Vision" in bibtex_ids
    assert "Brown2021GPT" in bibtex_ids


def test_extract_papers_missing_bib_skipped(caplog: pytest.LogCaptureFixture) -> None:
    """Missing BibTeX record is logged as WARNING and skipped."""
    import logging

    tex = _TEX.read_text()
    bib = _BIB.read_text()
    with caplog.at_level(logging.WARNING, logger="paper_sorts.services.import_service"):
        papers = list(import_service.extract_papers_from_tex_bib(tex, bib))
    # MissingEntry2022 has no bib record
    assert len(papers) == 3
    warning_messages = [r.message for r in caplog.records]
    assert any("MissingEntry2022" in str(m) or "not found" in str(m).lower() for m in warning_messages)


def test_bulk_import_via_cli(db_url: str) -> None:
    """pdbsearch import --tex --bib imports all valid papers into the DB."""
    result = runner.invoke(
        app,
        ["--database-url", db_url, "import", "--tex", str(_TEX), "--bib", str(_BIB)],
    )
    assert result.exit_code == 0
    assert "Imported 3 papers" in result.output or "3" in result.output

    with with_session(db_url) as session:
        repo = PaperRepository(session)
        nlp_results = repo.search_by_title("NLP Survey")
    assert len(nlp_results) == 1
    assert nlp_results[0].bibtex_id == "Smith2020NLP"


def test_bulk_import_per_paper_commit(db_url: str) -> None:
    """Per-paper commits mean a later failure preserves previously imported papers."""
    # Import once successfully
    result = runner.invoke(
        app,
        ["--database-url", db_url, "import", "--tex", str(_TEX), "--bib", str(_BIB)],
    )
    assert result.exit_code == 0

    # Import again — all three will fail with IntegrityError (duplicate bibtex_id)
    # but the originally imported papers are still in the DB
    result2 = runner.invoke(
        app,
        ["--database-url", db_url, "import", "--tex", str(_TEX), "--bib", str(_BIB)],
    )
    assert result2.exit_code == 0

    # Original papers should still be there
    with with_session(db_url) as session:
        repo = PaperRepository(session)
        nlp_results = repo.search_by_title("NLP Survey")
    assert len(nlp_results) == 1


def test_import_author_names_extracted(db_url: str) -> None:
    """Authors are correctly extracted from BibTeX entries."""
    tex = _TEX.read_text()
    bib = _BIB.read_text()
    papers = list(import_service.extract_papers_from_tex_bib(tex, bib))
    nlp_paper = next(p for p in papers if p.bibtex_id == "Smith2020NLP")
    assert len(nlp_paper.authors) >= 1
    # At least one of the expected authors should appear
    assert any("Smith" in a or "Johnson" in a for a in nlp_paper.authors)
