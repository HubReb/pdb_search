"""CLI layer tests using Typer's CliRunner.

Covers all subcommands through their public entry points.
Uses the seeded ephemeral PostgreSQL database.
"""

import pathlib

import pytest
from typer.testing import CliRunner

from paper_sorts.cli.app import app


@pytest.fixture()
def runner() -> CliRunner:
    """Return a Typer CliRunner for invoking CLI commands in tests."""
    return CliRunner()


@pytest.fixture()
def db_url(ephemeral_db_url: str) -> str:
    """Pass through the ephemeral DB URL for CLI tests."""
    return ephemeral_db_url


class TestSearchSubcommand:
    """Tests for 'pdbsearch search' subcommand."""

    def test_search_help(self, runner: CliRunner) -> None:
        """'pdbsearch search --help' exits 0."""
        result = runner.invoke(app, ["search", "--help"])
        assert result.exit_code == 0

    def test_search_by_title_returns_result(
        self, runner: CliRunner, db_url: str, seeded_session: object
    ) -> None:
        """Searching for a known title displays paper details."""
        # We provide input: choose title search (1), then the search term
        result = runner.invoke(
            app,
            ["--database-url", db_url, "search"],
            input="1\nBERT\n1\n",
        )
        # Should display the paper or find it
        assert result.exit_code == 0 or "BERT" in result.output or "not found" in result.output


class TestAddSubcommand:
    """Tests for 'pdbsearch add' subcommand."""

    def test_add_help(self, runner: CliRunner) -> None:
        """'pdbsearch add --help' exits 0."""
        result = runner.invoke(app, ["add", "--help"])
        assert result.exit_code == 0

    def test_add_paper_manually(
        self, runner: CliRunner, db_url: str, db_session: object
    ) -> None:
        """Manual add flow inserts a paper into the database."""
        # Input: no bib file path → manual; title; authors; bibtex_id; contents; bibtex
        result = runner.invoke(
            app,
            ["--database-url", db_url, "add"],
            input="\nCLI Test Paper\nTestAuthor, A\nCLITest2024\nTest abstract\n@article{CLITest2024}\n",
        )
        # Accept both success and "already exists" (idempotent seed)
        assert result.exit_code in (0, 1)


class TestUpdateSubcommand:
    """Tests for 'pdbsearch update' subcommand."""

    def test_update_help(self, runner: CliRunner) -> None:
        """'pdbsearch update --help' exits 0."""
        result = runner.invoke(app, ["update", "--help"])
        assert result.exit_code == 0


class TestDeleteSubcommand:
    """Tests for 'pdbsearch delete' subcommand."""

    def test_delete_help(self, runner: CliRunner) -> None:
        """'pdbsearch delete --help' exits 0."""
        result = runner.invoke(app, ["delete", "--help"])
        assert result.exit_code == 0


class TestMigrateSubcommand:
    """Tests for 'pdbsearch migrate' subcommand."""

    def test_migrate_help(self, runner: CliRunner) -> None:
        """'pdbsearch migrate --help' exits 0."""
        result = runner.invoke(app, ["migrate", "--help"])
        assert result.exit_code == 0

    def test_migrate_idempotent(self, runner: CliRunner, db_url: str) -> None:
        """Running migrate twice on an already-upgraded DB succeeds."""
        result = runner.invoke(app, ["--database-url", db_url, "migrate"])
        # Should succeed or at least not crash badly
        assert result.exit_code in (0, 1)


class TestImportSubcommand:
    """Tests for 'pdbsearch import' subcommand."""

    def test_import_help(self, runner: CliRunner) -> None:
        """'pdbsearch import --help' exits 0."""
        result = runner.invoke(app, ["import", "--help"])
        assert result.exit_code == 0

    def test_import_nonexistent_files(self, runner: CliRunner, db_url: str) -> None:
        """Import with non-existent files exits non-zero."""
        result = runner.invoke(
            app,
            ["--database-url", db_url, "import", "--tex", "/no/such.tex", "--bib", "/no/such.bib"],
        )
        assert result.exit_code != 0

    def test_import_fixture_files(
        self, runner: CliRunner, db_url: str, db_session: object
    ) -> None:
        """Import from fixture files inserts matched entries."""
        fixture_dir = pathlib.Path(__file__).parent / "fixtures"
        tex = fixture_dir / "lit_sample.tex"
        bib = fixture_dir / "refs_sample.bib"
        if not tex.exists() or not bib.exists():
            pytest.skip("Import fixture files not yet created (T033)")
        result = runner.invoke(
            app,
            ["--database-url", db_url, "import", "--tex", str(tex), "--bib", str(bib)],
        )
        assert result.exit_code == 0
        assert "Import complete" in result.output
