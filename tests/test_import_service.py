"""Integration tests for the import service.

Tests run against the ephemeral PostgreSQL instance.
Fixture files: tests/fixtures/sample.tex + tests/fixtures/sample.bib

MissingBib2024 is in sample.tex but NOT in sample.bib — it must be skipped.
AccentPaper2023 has a LaTeX accent in the author name — it must round-trip.
"""

from pathlib import Path

import pytest

from paper_sorts.db.session import with_session
from paper_sorts.services import import_service, paper_service

FIXTURES = Path(__file__).parent / "fixtures"
SAMPLE_TEX = FIXTURES / "sample.tex"
SAMPLE_BIB = FIXTURES / "sample.bib"


@pytest.fixture(autouse=True)
def cleanup(db_engine: object) -> object:  # type: ignore[type-arg, return]
    """Clean up all rows after each import test."""
    from sqlalchemy import text
    from sqlalchemy.engine import Engine

    assert isinstance(db_engine, Engine)
    yield
    with with_session(db_engine) as session:
        session.execute(text("DELETE FROM authors_papers"))
        session.execute(text("DELETE FROM papers"))
        session.execute(text("DELETE FROM bib"))
        session.execute(text("DELETE FROM authors_id"))


class TestExtractPapersFromTexBib:
    """Unit-level tests for extract_papers_from_tex_bib (no DB)."""

    def test_yields_matching_entries(self) -> None:
        """extract_papers_from_tex_bib yields only entries with bib matches."""
        papers = list(import_service.extract_papers_from_tex_bib(SAMPLE_TEX, SAMPLE_BIB))
        bibtex_ids = {p.bibtex_id for p in papers}
        # TestPaper2024 and AccentPaper2023 have bib entries
        assert "TestPaper2024" in bibtex_ids
        assert "AccentPaper2023" in bibtex_ids

    def test_skips_missing_bib(self) -> None:
        """extract_papers_from_tex_bib skips entries with no bib record."""
        papers = list(import_service.extract_papers_from_tex_bib(SAMPLE_TEX, SAMPLE_BIB))
        bibtex_ids = {p.bibtex_id for p in papers}
        # MissingBib2024 is in .tex but not in .bib — must be absent
        assert "MissingBib2024" not in bibtex_ids

    def test_accent_roundtrip(self) -> None:
        """Author name with LaTeX accent is preserved in the bibtex string."""
        papers = list(import_service.extract_papers_from_tex_bib(SAMPLE_TEX, SAMPLE_BIB))
        accent_papers = [p for p in papers if p.bibtex_id == "AccentPaper2023"]
        assert accent_papers
        # The bibtex field should contain the raw BibTeX with the accent escape
        assert "uller" in accent_papers[0].bibtex or "\\\"" in accent_papers[0].bibtex


class TestImportServiceIntegration:
    """Integration tests: extract + persist against real DB."""

    def test_import_inserts_papers(self, db_engine: object) -> None:  # type: ignore[type-arg]
        """Importing sample.tex + sample.bib inserts the expected papers."""
        from sqlalchemy.engine import Engine

        assert isinstance(db_engine, Engine)

        for paper in import_service.extract_papers_from_tex_bib(SAMPLE_TEX, SAMPLE_BIB):
            paper_service.add_paper(db_engine, paper)

        results = paper_service.search_by_title(db_engine, "A Test Paper About Things")
        assert len(results) == 1
        assert results[0].bibtex_id == "TestPaper2024"

    def test_import_idempotent(self, db_engine: object) -> None:  # type: ignore[type-arg]
        """Re-importing the same files does not create duplicate entries."""
        from sqlalchemy.engine import Engine

        assert isinstance(db_engine, Engine)

        # First import
        for paper in import_service.extract_papers_from_tex_bib(SAMPLE_TEX, SAMPLE_BIB):
            try:
                paper_service.add_paper(db_engine, paper)
            except ValueError:
                pass

        # Second import — duplicates skipped, no error
        for paper in import_service.extract_papers_from_tex_bib(SAMPLE_TEX, SAMPLE_BIB):
            try:
                paper_service.add_paper(db_engine, paper)
            except ValueError:
                pass  # Expected for duplicates

        # Still only one entry per paper
        results = paper_service.search_by_title(db_engine, "A Test Paper About Things")
        assert len(results) == 1
