"""Unit tests for paper_sorts.services.import_service.

Tests operate on the fixture files tests/fixtures/sample.tex and sample.bib.
No database required.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from paper_sorts.services.import_service import extract_papers_from_tex_bib

FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestExtractPapersFromTexBib:
    """Tests for extract_papers_from_tex_bib."""

    def test_yields_matched_entries(self) -> None:
        """Yields PaperCreate for each entry with a matching .bib record."""
        papers = list(
            extract_papers_from_tex_bib(
                FIXTURES_DIR / "sample.tex",
                FIXTURES_DIR / "sample.bib",
            )
        )
        # sample.tex has 3 citations; NoBib2024 has no .bib entry → 2 papers expected
        assert len(papers) == 2

    def test_matched_bibtex_ids(self) -> None:
        """Yielded papers have the correct bibtex_ids."""
        papers = list(
            extract_papers_from_tex_bib(
                FIXTURES_DIR / "sample.tex",
                FIXTURES_DIR / "sample.bib",
            )
        )
        keys = {p.bibtex_id for p in papers}
        assert "Sample2024A" in keys
        assert "Sample2024B" in keys
        # NoBib2024 must NOT be present
        assert "NoBib2024" not in keys

    def test_missing_bib_skipped_with_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Entry with no matching .bib record is skipped and logged as warning."""
        import logging

        with caplog.at_level(logging.WARNING, logger="paper_sorts.services.import_service"):
            list(
                extract_papers_from_tex_bib(
                    FIXTURES_DIR / "sample.tex",
                    FIXTURES_DIR / "sample.bib",
                )
            )
        warning_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any("NoBib2024" in msg for msg in warning_messages)

    def test_authors_populated(self) -> None:
        """Yields papers with non-empty author lists."""
        papers = list(
            extract_papers_from_tex_bib(
                FIXTURES_DIR / "sample.tex",
                FIXTURES_DIR / "sample.bib",
            )
        )
        for p in papers:
            assert len(p.authors) >= 1

    def test_contents_populated(self) -> None:
        """Yields papers with non-empty contents."""
        papers = list(
            extract_papers_from_tex_bib(
                FIXTURES_DIR / "sample.tex",
                FIXTURES_DIR / "sample.bib",
            )
        )
        for p in papers:
            assert p.contents.strip() != ""
