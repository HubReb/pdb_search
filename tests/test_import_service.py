"""Integration tests for import_service.extract_papers_from_tex_bib."""

from __future__ import annotations

import os

import pytest

from paper_sorts.services.import_service import extract_papers_from_tex_bib

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")
TEX_FILE = os.path.join(FIXTURES_DIR, "literature_overview.tex")
BIB_FILE = os.path.join(FIXTURES_DIR, "bib.bib")


class TestExtractPapersFromTexBib:
    """Tests for extract_papers_from_tex_bib."""

    def test_returns_matched_entries(self) -> None:
        """Entries with matching BibTeX records are returned."""
        papers = list(extract_papers_from_tex_bib(TEX_FILE, BIB_FILE))
        keys = {p.bibtex_id for p in papers}
        assert "TexImport2026A" in keys
        assert "TexImport2026B" in keys

    def test_skips_missing_bibtex_key(self, caplog: pytest.LogCaptureFixture) -> None:
        """An entry with no matching BibTeX record is skipped with a warning."""
        import logging

        with caplog.at_level(logging.WARNING, logger="paper_sorts.services.import_service"):
            papers = list(extract_papers_from_tex_bib(TEX_FILE, BIB_FILE))

        keys = {p.bibtex_id for p in papers}
        assert "MissingKey2026" not in keys
        assert any("MissingKey2026" in record.message for record in caplog.records)

    def test_paper_has_authors(self) -> None:
        """Each matched paper has at least one author."""
        papers = list(extract_papers_from_tex_bib(TEX_FILE, BIB_FILE))
        for paper in papers:
            assert len(paper.authors) >= 1

    def test_bibtex_roundtrip(self) -> None:
        """Each matched paper has a non-empty bibtex string."""
        papers = list(extract_papers_from_tex_bib(TEX_FILE, BIB_FILE))
        for paper in papers:
            assert paper.bibtex.strip()

    def test_tex_not_found_raises(self) -> None:
        """FileNotFoundError is raised when the .tex file does not exist."""
        with pytest.raises(FileNotFoundError):
            list(extract_papers_from_tex_bib("/nonexistent/file.tex", BIB_FILE))

    def test_bib_not_found_raises(self) -> None:
        """FileNotFoundError is raised when the .bib file does not exist."""
        with pytest.raises(FileNotFoundError):
            list(extract_papers_from_tex_bib(TEX_FILE, "/nonexistent/file.bib"))
