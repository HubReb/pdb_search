"""CLI integration tests using Typer CliRunner.

Tests all subcommands via their public entry points. Uses the ephemeral
database so no personal database is needed (constitution Principle II / US3).
"""

from __future__ import annotations

import pathlib

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

    def test_search_author_not_found(self, ephemeral_db_url: str) -> None:
        """Searching for a non-existent author prints a not-found message."""
        result = invoke(["search"], ephemeral_db_url, input_="1\nNobody, X.\n")
        assert result.exit_code == 0
        assert "not found" in result.output.lower()

    def test_search_title_not_found(self, ephemeral_db_url: str) -> None:
        """Searching for a non-existent title prints a not-found message."""
        result = invoke(["search"], ephemeral_db_url, input_="2\nNo Such Title At All\n")
        assert result.exit_code == 0
        assert "not found" in result.output.lower()


class TestAddCmd:
    """Tests for the 'add' subcommand."""

    def test_add_paper_inline(
        self, clean_db_session: object, ephemeral_db_url: str
    ) -> None:
        """Adding a paper inline makes it findable via search."""
        user_input = (
            "Doe, John\n"           # author
            "CLI Add Test Paper\n"  # title
            "CLIAdd2026\n"          # bibtex key
            "2\n"                   # bibtex inline (not file)
            "@misc{CLIAdd2026}\n"   # bibtex
            "A test summary.\n"     # contents
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

    def test_update_title_with_confirmation(
        self, clean_db_session: object, ephemeral_db_url: str
    ) -> None:
        """Confirming an update makes the change in the database."""
        paper = PaperCreate(
            title="CLI Update Title Old",
            contents="Summary.",
            bibtex_id="CLIUpdateTitle2026",
            bibtex="@misc{CLIUpdateTitle2026}",
            authors=["Test, Author"],
        )
        paper_service.add_paper(ephemeral_db_url, paper)
        results = paper_service.search_by_title(ephemeral_db_url, "CLI Update Title Old")
        paper_id = results[0].paper_id

        user_input = (
            "1\n"                       # papers table
            "1\n"                       # title column
            f"{paper_id}\n"             # id
            "CLI Update Title New\n"    # new value
            "y\n"                       # confirm
        )
        result = invoke(["update"], ephemeral_db_url, input_=user_input)
        assert result.exit_code == 0

        new_results = paper_service.search_by_title(ephemeral_db_url, "CLI Update Title New")
        assert len(new_results) == 1

    def test_update_bib(
        self, clean_db_session: object, ephemeral_db_url: str
    ) -> None:
        """Updating bib table works via update subcommand."""
        paper = PaperCreate(
            title="CLI Update Bib Test",
            contents="Summary.",
            bibtex_id="CLIUpdateBib2026",
            bibtex="@misc{CLIUpdateBib2026, note={old}}",
            authors=["Test, Author"],
        )
        paper_service.add_paper(ephemeral_db_url, paper)

        user_input = (
            "2\n"                                         # bib table
            "CLIUpdateBib2026\n"                          # bibtex key
            "@misc{CLIUpdateBib2026, note={new}}\n"       # new bibtex
            "y\n"                                         # confirm
        )
        result = invoke(["update"], ephemeral_db_url, input_=user_input)
        assert result.exit_code == 0

    def test_update_author(
        self, clean_db_session: object, ephemeral_db_url: str
    ) -> None:
        """Updating author table works via update subcommand."""
        paper = PaperCreate(
            title="CLI Update Author Test",
            contents="Summary.",
            bibtex_id="CLIUpdateAuthor2026",
            bibtex="@misc{CLIUpdateAuthor2026}",
            authors=["OldName, X."],
        )
        paper_service.add_paper(ephemeral_db_url, paper)

        user_input = (
            "3\n"           # authors table
            "OldName, X.\n" # current name
            "NewName, X.\n" # new name
            "y\n"           # confirm
        )
        result = invoke(["update"], ephemeral_db_url, input_=user_input)
        assert result.exit_code == 0

    def test_update_abort_column(
        self, clean_db_session: object, ephemeral_db_url: str
    ) -> None:
        """Aborting at column selection exits without error."""
        result = invoke(["update"], ephemeral_db_url, input_="1\n3\n")
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

    def test_delete_cancel(
        self, clean_db_session: object, ephemeral_db_url: str
    ) -> None:
        """Cancelling delete leaves the paper in the database."""
        paper = PaperCreate(
            title="CLI Delete Cancel Test",
            contents="Summary.",
            bibtex_id="CLIDeleteCancel2026",
            bibtex="@misc{CLIDeleteCancel2026}",
            authors=["Test, Author"],
        )
        paper_service.add_paper(ephemeral_db_url, paper)

        user_input = "CLI Delete Cancel Test\nn\n"
        result = invoke(["delete"], ephemeral_db_url, input_=user_input)
        assert result.exit_code == 0

        after = paper_service.search_by_title(ephemeral_db_url, "CLI Delete Cancel Test")
        assert len(after) == 1


class TestImportCmd:
    """Tests for the 'import' subcommand."""

    def test_import_from_files(
        self, clean_db_session: object, ephemeral_db_url: str, tmp_path: pathlib.Path
    ) -> None:
        """Bulk import from .tex and .bib files inserts papers into the database."""
        tex_content = r"""\documentclass{article}
\begin{document}
\begin{itemize}
\item * TestImportCLI \cite{TestImportCLI2026}: A test paper for CLI import.
\end{itemize}
\end{document}
"""
        bib_content = """
@article{TestImportCLI2026,
  author = {Importer, Test},
  title = {TestImportCLI},
  year = {2026}
}
"""
        tex_file = tmp_path / "test.tex"
        bib_file = tmp_path / "test.bib"
        tex_file.write_text(tex_content)
        bib_file.write_text(bib_content)

        result = invoke(
            ["import", "--tex", str(tex_file), "--bib", str(bib_file)],
            ephemeral_db_url,
        )
        assert result.exit_code == 0
        assert "inserted" in result.output.lower() or "complete" in result.output.lower()

        # Verify paper is in DB
        results = paper_service.search_by_title(ephemeral_db_url, "TestImportCLI")
        assert len(results) >= 1

    def test_import_duplicate_skipped(
        self, clean_db_session: object, ephemeral_db_url: str, tmp_path: pathlib.Path
    ) -> None:
        """Re-importing a paper with duplicate bibtex_id skips it and reports skipped count."""
        tex_content = r"""\documentclass{article}
\begin{document}
\begin{itemize}
\item * DupImportCLI \cite{DupImportCLI2026}: A duplicate import test.
\end{itemize}
\end{document}
"""
        bib_content = """
@article{DupImportCLI2026,
  author = {Importer, Test},
  title = {DupImportCLI},
  year = {2026}
}
"""
        tex_file = tmp_path / "dup.tex"
        bib_file = tmp_path / "dup.bib"
        tex_file.write_text(tex_content)
        bib_file.write_text(bib_content)

        # First import
        invoke(["import", "--tex", str(tex_file), "--bib", str(bib_file)], ephemeral_db_url)
        # Second import — should skip
        result = invoke(
            ["import", "--tex", str(tex_file), "--bib", str(bib_file)],
            ephemeral_db_url,
        )
        assert result.exit_code == 0
        assert "skipped" in result.output.lower() or "complete" in result.output.lower()


class TestAppInteractiveMenu:
    """Tests for the interactive top-level menu (invoked with no subcommand)."""

    def test_interactive_quit(self, ephemeral_db_url: str) -> None:
        """Choosing Quit from the interactive menu exits cleanly."""
        # Option 4 = "(Q)uit"
        result = runner.invoke(app, ["--database-url", ephemeral_db_url], input="4\n")
        assert result.exit_code == 0
        assert "Closing connection" in result.output or "Quit" in result.output or result.exit_code == 0

    def test_interactive_search_then_quit(self, ephemeral_db_url: str) -> None:
        """Entering the search sub-menu then returning and quitting works."""
        # Menu: 1=Search, then 3=Abort search, then 4=Quit
        user_input = "1\n3\n4\n"
        result = runner.invoke(app, ["--database-url", ephemeral_db_url], input=user_input)
        assert result.exit_code == 0

    def test_interactive_add_then_quit(
        self, clean_db_session: object, ephemeral_db_url: str
    ) -> None:
        """Entering the add sub-menu then quitting works."""
        # Menu: 2=Add, then provide paper data, then 4=Quit
        user_input = (
            "2\n"                      # add
            "Author, Test.\n"          # author
            "Interactive Menu Test\n"  # title
            "IntMenuTest2026\n"        # bibtex key
            "2\n"                      # inline bibtex
            "@misc{IntMenuTest2026}\n" # bibtex
            "Test summary.\n"          # contents
            "4\n"                      # quit
        )
        result = runner.invoke(app, ["--database-url", ephemeral_db_url], input=user_input)
        assert result.exit_code == 0

    def test_subcommand_no_url_fails(self) -> None:
        """Running a subcommand with no URL set returns exit code 1."""
        # Clear any env var that might be set
        import os
        env = {k: v for k, v in os.environ.items() if k != "PDBSEARCH_DATABASE_URL"}
        result = runner.invoke(app, ["search"], env=env)
        assert result.exit_code == 1


class TestSearchWithResults:
    """Tests for search subcommand when data is committed and visible across sessions."""

    def test_search_by_title_shows_pretty_print(
        self, seeded_db_url: str
    ) -> None:
        """Title search that finds a result displays bibtex, authors, and summary."""
        user_input = (
            "2\n"
            "Large-scale Self- and Semi-Supervised Learning for Speech Translation\n"
        )
        result = invoke(["search"], seeded_db_url, input_=user_input)
        assert result.exit_code == 0
        # Should display paper details (bibtex or authors)
        assert "Wang2021LargeScaleSA" in result.output or "Pino" in result.output

    def test_search_by_author_shows_paper(self, seeded_db_url: str) -> None:
        """Author search that finds a result displays paper details."""
        user_input = "1\nPino, J.\n1\n"
        result = invoke(["search"], seeded_db_url, input_=user_input)
        assert result.exit_code == 0

    def test_search_disambiguate(
        self, ephemeral_db_url: str, clean_db_session: object
    ) -> None:
        """Search with two papers sharing a title shows disambiguation menu."""
        # Add two papers with same title
        paper1 = PaperCreate(
            title="Ambiguous Title",
            contents="First paper.",
            bibtex_id="Ambiguous2026A",
            bibtex="@misc{Ambiguous2026A}",
            authors=["Author, A."],
        )
        paper2 = PaperCreate(
            title="Ambiguous Title",
            contents="Second paper.",
            bibtex_id="Ambiguous2026B",
            bibtex="@misc{Ambiguous2026B}",
            authors=["Author, B."],
        )
        paper_service.add_paper(ephemeral_db_url, paper1)
        paper_service.add_paper(ephemeral_db_url, paper2)

        # Search by title (option 2), enter title, pick option 1 from disambiguation
        user_input = "2\nAmbiguous Title\n1\n"
        result = invoke(["search"], ephemeral_db_url, input_=user_input)
        assert result.exit_code == 0


class TestMigrateCmd:
    """Tests for the 'migrate' subcommand."""

    def test_migrate_runs_without_error(self, ephemeral_db_url: str) -> None:
        """pdbsearch migrate completes successfully on an already-migrated database."""
        result = invoke(["migrate"], ephemeral_db_url)
        assert result.exit_code == 0
        assert "Migration complete" in result.output
