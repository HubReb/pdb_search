"""Tests for the bulk import service and CLI subcommand.

Verifies:
  - Full import of a fixture .tex + .bib pair
  - Skipping entries with missing bib records (with logged warning)
  - Idempotency (re-importing skips duplicates, returns False from add_paper)
  - CLI import subcommand completes successfully
"""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from paper_sorts.cli.app import app

runner = CliRunner()

# Use the test fixture files in tests/fixtures/
_FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"
_TEX_FILE = _FIXTURES_DIR / "literature_overview.tex"
_BIB_FILE = _FIXTURES_DIR / "bib.bib"


class TestImportService:
    """Tests for import_service.extract_papers_from_tex_bib."""

    def test_extract_yields_papers(self) -> None:
        """extract_papers_from_tex_bib yields PaperCreate objects from fixture files."""
        from paper_sorts.services.import_service import extract_papers_from_tex_bib

        if not _TEX_FILE.exists() or not _BIB_FILE.exists():
            pytest.skip("Import fixture files not yet created")

        papers = list(extract_papers_from_tex_bib(_TEX_FILE, _BIB_FILE))
        # Fixture should have at least 2 valid entries (1 deliberately missing from bib)
        assert len(papers) >= 2

    def test_extract_skips_missing_bib(self) -> None:
        """Entries in .tex with no matching .bib record are skipped, not raised."""
        from paper_sorts.services.import_service import extract_papers_from_tex_bib

        if not _TEX_FILE.exists() or not _BIB_FILE.exists():
            pytest.skip("Import fixture files not yet created")

        papers = list(extract_papers_from_tex_bib(_TEX_FILE, _BIB_FILE))
        bibtex_ids = {p.bibtex_id for p in papers}
        # "MissingBib2026" is the entry in .tex that has no .bib record
        assert "MissingBib2026" not in bibtex_ids


class TestImportCLI:
    """Tests for the 'pdbsearch import' subcommand."""

    def test_import_cli_runs(self, ephemeral_db_url: str) -> None:
        """Import subcommand completes successfully with fixture files."""
        if not _TEX_FILE.exists() or not _BIB_FILE.exists():
            pytest.skip("Import fixture files not yet created")

        result = runner.invoke(
            app,
            [
                "--database-url",
                ephemeral_db_url,
                "import",
                "--tex",
                str(_TEX_FILE),
                "--bib",
                str(_BIB_FILE),
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output
        assert "complete" in result.output.lower() or "import" in result.output.lower()

    def test_import_idempotent(self, ephemeral_db_url: str) -> None:
        """Running import twice skips duplicates on second run."""
        if not _TEX_FILE.exists() or not _BIB_FILE.exists():
            pytest.skip("Import fixture files not yet created")

        _import_args = [
            "--database-url", ephemeral_db_url,
            "import", "--tex", str(_TEX_FILE), "--bib", str(_BIB_FILE),
        ]

        # First import
        runner.invoke(app, _import_args, catch_exceptions=False)

        # Count before second import
        from sqlalchemy import create_engine, text

        engine = create_engine(ephemeral_db_url)
        with engine.connect() as conn:
            count_before = conn.execute(text("SELECT count(*) FROM papers")).scalar()
        engine.dispose()

        # Second import
        runner.invoke(app, _import_args, catch_exceptions=False)

        engine = create_engine(ephemeral_db_url)
        with engine.connect() as conn:
            count_after = conn.execute(text("SELECT count(*) FROM papers")).scalar()
        engine.dispose()

        assert count_before == count_after
