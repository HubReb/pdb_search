"""Integration tests for bulk import service and CLI.

Tests:
- Full import: 2 of 3 entries inserted (1 with no matching .bib skipped)
- Duplicate key skip: already-existing key is not re-inserted
- Missing bib record: entry in .tex with no matching .bib is skipped with warning
- Partial failure: entries inserted before failure are preserved

Fixtures:
- tests/fixtures/sample.tex: 3 paper citations (one with no .bib match)
- tests/fixtures/sample.bib: 2 BibTeX entries
"""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import Engine

from paper_sorts.db.repositories import PaperRepository
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service
from paper_sorts.services.import_service import extract_papers_from_tex_bib

FIXTURE_DIR = Path(__file__).parent / "fixtures"
TEX_FILE = FIXTURE_DIR / "sample.tex"
BIB_FILE = FIXTURE_DIR / "sample.bib"


class TestExtractPapersFromTexBib:
    """Unit-level tests for the import_service extraction function."""

    def test_extracts_matched_entries(self) -> None:
        """Extraction yields only papers that have matching .bib entries."""
        papers = list(extract_papers_from_tex_bib(TEX_FILE, BIB_FILE))
        # Only 2 of 3 have matching .bib records
        assert len(papers) == 2

    def test_extracted_papers_have_required_fields(self) -> None:
        """Extracted papers have non-empty title, bibtex_id, bibtex, and authors."""
        papers = list(extract_papers_from_tex_bib(TEX_FILE, BIB_FILE))
        for paper in papers:
            assert paper.bibtex_id, f"Missing bibtex_id for: {paper.title}"
            assert paper.bibtex, f"Missing bibtex for: {paper.title}"
            assert paper.authors, f"Missing authors for: {paper.title}"

    def test_known_bibtex_ids_present(self) -> None:
        """Extraction yields papers for Sample2024First and Sample2024Second."""
        papers = list(extract_papers_from_tex_bib(TEX_FILE, BIB_FILE))
        ids = {p.bibtex_id for p in papers}
        assert "Sample2024First" in ids
        assert "Sample2024Second" in ids

    def test_missing_bib_skipped(self) -> None:
        """Entry with no matching .bib record is skipped (not yielded)."""
        papers = list(extract_papers_from_tex_bib(TEX_FILE, BIB_FILE))
        ids = {p.bibtex_id for p in papers}
        assert "MissingBib2024" not in ids

    def test_missing_tex_file_raises(self) -> None:
        """FileNotFoundError raised if .tex file does not exist."""
        with pytest.raises(FileNotFoundError, match=r"\.tex"):
            list(extract_papers_from_tex_bib(Path("/nonexistent/file.tex"), BIB_FILE))

    def test_missing_bib_file_raises(self) -> None:
        """FileNotFoundError raised if .bib file does not exist."""
        with pytest.raises(FileNotFoundError, match=r"\.bib"):
            list(extract_papers_from_tex_bib(TEX_FILE, Path("/nonexistent/file.bib")))


class TestBulkImportIntegration:
    """Integration tests for bulk import against the ephemeral DB."""

    def test_full_import_inserts_matched_papers(self, clean_engine: Engine) -> None:
        """Full import inserts 2 papers (the 2 with matching .bib records)."""
        papers = list(extract_papers_from_tex_bib(TEX_FILE, BIB_FILE))

        for p in papers:
            with with_session(clean_engine) as session:
                paper_service.add_paper(session, p)

        with with_session(clean_engine) as session:
            repo = PaperRepository(session)
            assert repo.get_by_bibtex_id("Sample2024First") is not None
            assert repo.get_by_bibtex_id("Sample2024Second") is not None
            assert repo.get_by_bibtex_id("MissingBib2024") is None

    def test_duplicate_key_skip_does_not_duplicate(self, clean_engine: Engine) -> None:
        """Attempting to import an already-inserted paper skips it (ValueError caught)."""
        papers = list(extract_papers_from_tex_bib(TEX_FILE, BIB_FILE))
        assert papers  # sanity

        # Insert first paper
        with with_session(clean_engine) as session:
            paper_service.add_paper(session, papers[0])

        # Attempt to insert again — should raise ValueError
        with pytest.raises(ValueError, match="already exists"):
            with with_session(clean_engine) as session:
                paper_service.add_paper(session, papers[0])

        # Only one record should exist
        with with_session(clean_engine) as session:
            results = PaperRepository(session).search_by_title(papers[0].title)
        assert len(results) == 1

    def test_partial_failure_preserves_prior_entries(self, clean_engine: Engine) -> None:
        """Entries successfully inserted before a failure are not rolled back."""
        papers = list(extract_papers_from_tex_bib(TEX_FILE, BIB_FILE))
        assert len(papers) >= 2

        # Insert first paper successfully
        with with_session(clean_engine) as session:
            paper_service.add_paper(session, papers[0])

        # Second insert fails (duplicate — same paper)
        try:
            with with_session(clean_engine) as session:
                paper_service.add_paper(session, papers[0])  # will fail
        except ValueError:
            pass  # expected

        # First paper is still there (per-paper commit semantics)
        with with_session(clean_engine) as session:
            result = PaperRepository(session).get_by_bibtex_id(papers[0].bibtex_id)
        assert result is not None

    def test_import_authors_linked(self, clean_engine: Engine) -> None:
        """Imported papers are searchable by author."""
        papers = list(extract_papers_from_tex_bib(TEX_FILE, BIB_FILE))
        for p in papers:
            with with_session(clean_engine) as session:
                paper_service.add_paper(session, p)

        # Sample2024First has authors Smith, John and Jones, Mary
        with with_session(clean_engine) as session:
            results = paper_service.search_by_author(session, "Smith, John")
        assert len(results) >= 1
        assert any(r.bibtex_id == "Sample2024First" for r in results)


class TestImportCLI:
    """CLI-level tests for the `pdbsearch import` subcommand."""

    def test_import_via_cli(self, clean_engine: Engine) -> None:
        """Import subcommand inserts matched papers and reports results."""
        from typer.testing import CliRunner

        from paper_sorts.cli.app import app

        runner = CliRunner()
        db_url = str(clean_engine.url)

        result = runner.invoke(
            app,
            [
                "--database-url", db_url,
                "import",
                "--tex", str(TEX_FILE),
                "--bib", str(BIB_FILE),
            ],
        )
        assert result.exit_code == 0
        assert "Traceback" not in result.output
        assert "inserted" in result.output.lower()

        # Verify papers are in DB
        with with_session(clean_engine) as session:
            repo = PaperRepository(session)
            assert repo.get_by_bibtex_id("Sample2024First") is not None
            assert repo.get_by_bibtex_id("Sample2024Second") is not None
