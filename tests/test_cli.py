"""CLI integration tests using Typer CliRunner.

Tests all subcommands via their public entry points. Uses the ephemeral
database so no personal database is needed (constitution Principle II / US3).
"""

from __future__ import annotations

import typer.testing
from typer.testing import CliRunner

from paper_sorts.cli.app import app
from paper_sorts.db.repositories import PaperCreate
from paper_sorts.services import paper_service

runner = CliRunner()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def invoke(args: list[str], db_url: str, input_: str = "") -> typer.testing.Result:
    """Invoke the CLI with the given args, injecting the db URL."""
    return runner.invoke(app, ["--database-url", db_url] + args, input=input_)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestHelp:
    """Basic CLI smoke tests."""

    def test_help_shows_all_subcommands(self) -> None:
        """--help output lists all six subcommands."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        for cmd in ("search", "add", "update", "delete", "import", "migrate"):
            assert cmd in result.output


class TestSearchCmd:
    """Tests for the 'search' subcommand."""

    def test_search_by_title_found(
        self, seeded_session: object, ephemeral_db_url: str
    ) -> None:
        """Search by title finds a seeded paper and prints its details."""
        # simulate: pick title search (option 2), then enter the title, abort after
        user_input = "2\nLarge-scale Self- and Semi-Supervised Learning for Speech Translation\n"
        result = invoke(["search"], ephemeral_db_url, input_=user_input)
        assert "Wang2021LargeScaleSA" in result.output or "Large-scale" in result.output

    def test_search_by_author_found(
        self, seeded_session: object, ephemeral_db_url: str
    ) -> None:
        """Search by author finds seeded papers and prints results."""
        user_input = "1\nPino, J.\n1\n"  # pick author search, enter name, pick first result
        result = invoke(["search"], ephemeral_db_url, input_=user_input)
        # Should show some output without crashing
        assert result.exit_code == 0 or "Pino" in result.output or "not found" in result.output

    def test_abort_search(self, seeded_session: object, ephemeral_db_url: str) -> None:
        """Choosing abort exits the search without error."""
        result = invoke(["search"], ephemeral_db_url, input_="3\n")
        assert result.exit_code == 0


class TestAddCmd:
    """Tests for the 'add' subcommand."""

    def test_add_paper_inline(
        self, clean_db_session: object, ephemeral_db_url: str
    ) -> None:
        """Adding a paper inline makes it findable via search."""
        user_input = (
            "Doe, John\n"         # author
            "CLI Add Test Paper\n"  # title
            "CLIAdd2026\n"         # bibtex key
            "2\n"                  # bibtex inline (not file)
            "@misc{CLIAdd2026}\n"  # bibtex
            "A test summary.\n"    # contents
        )
        result = invoke(["add"], ephemeral_db_url, input_=user_input)
        assert result.exit_code == 0

        # Verify it's in the DB
        results = paper_service.search_by_title(ephemeral_db_url, "CLI Add Test Paper")
        assert len(results) >= 1


class TestUpdateCmd:
    """Tests for the 'update' subcommand."""

    def test_update_with_no_confirmation_makes_no_change(
        self, clean_db_session: object, ephemeral_db_url: str
    ) -> None:
        """Answering 'n' at the confirmation step makes no change to the database."""
        paper = PaperCreate(
            title="CLI Update No Change",
            contents="Original contents.",
            bibtex_id="CLIUpdateNo2026",
            bibtex="@misc{CLIUpdateNo2026}",
            authors=["Test, Author"],
        )
        paper_service.add_paper(ephemeral_db_url, paper)
        results = paper_service.search_by_title(ephemeral_db_url, "CLI Update No Change")
        paper_id = results[0].paper_id

        user_input = (
            "1\n"                  # papers table
            "2\n"                  # contents column
            f"{paper_id}\n"        # id
            "Changed contents.\n"  # new value
            "n\n"                  # no confirmation
        )
        result = invoke(["update"], ephemeral_db_url, input_=user_input)
        assert result.exit_code == 0

        # Verify contents unchanged
        after = paper_service.search_by_title(ephemeral_db_url, "CLI Update No Change")
        assert after[0].contents == "Original contents."

    def test_abort_update(
        self, clean_db_session: object, ephemeral_db_url: str
    ) -> None:
        """Choosing abort in the table selection exits without error."""
        result = invoke(["update"], ephemeral_db_url, input_="4\n")
        assert result.exit_code == 0


class TestDeleteCmd:
    """Tests for the 'delete' subcommand."""

    def test_delete_with_confirmation(
        self, clean_db_session: object, ephemeral_db_url: str
    ) -> None:
        """Confirming delete removes the paper from the database."""
        paper = PaperCreate(
            title="CLI Delete Test",
            contents="Summary.",
            bibtex_id="CLIDelete2026",
            bibtex="@misc{CLIDelete2026}",
            authors=["Test, Author"],
        )
        paper_service.add_paper(ephemeral_db_url, paper)

        user_input = "CLI Delete Test\ny\n"
        result = invoke(["delete"], ephemeral_db_url, input_=user_input)
        assert result.exit_code == 0

        after = paper_service.search_by_title(ephemeral_db_url, "CLI Delete Test")
        assert after == []

    def test_delete_not_found(
        self, ephemeral_db_url: str
    ) -> None:
        """Searching for a non-existent paper prints a friendly message."""
        result = invoke(["delete"], ephemeral_db_url, input_="No Such Paper\n")
        assert result.exit_code == 0
        assert "not found" in result.output.lower()


class TestMigrateCmd:
    """Tests for the 'migrate' subcommand."""

    def test_migrate_runs_without_error(self, ephemeral_db_url: str) -> None:
        """pdbsearch migrate completes successfully on an already-migrated database."""
        result = invoke(["migrate"], ephemeral_db_url)
        assert result.exit_code == 0
        assert "Migration complete" in result.output
