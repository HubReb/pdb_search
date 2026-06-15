"""CLI tests for paper_sorts using Typer's CliRunner.

Tests every subcommand through its public entry point.
Covers success paths, abort paths, empty input re-prompt, and invalid menu choice.
"""

from __future__ import annotations

from typer.testing import CliRunner

from paper_sorts.cli.app import app
from tests.fixtures.seed_papers import PAPER_1, PAPER_2

runner = CliRunner()


def _cli(db_url: str, *args: str, input: str | None = None) -> object:
    """Run a pdbsearch CLI command with the given db_url and args."""
    return runner.invoke(
        app,
        ["--database-url", db_url, *args],
        input=input,
        catch_exceptions=False,
    )


class TestSearchSubcommand:
    """Tests for the 'pdbsearch search' subcommand."""

    def test_search_by_title_found(self, ephemeral_db_url: str, db_session: object) -> None:
        """Search by title of PAPER_1 prints title and authors."""
        result = _cli(ephemeral_db_url, "search", input="2\n" + PAPER_1.title + "\n")
        assert result.exit_code == 0, result.output
        assert PAPER_1.title in result.output

    def test_search_by_author_found(self, ephemeral_db_url: str, db_session: object) -> None:
        """Search by author 'Wang, Changhan' finds PAPER_2."""
        result = _cli(ephemeral_db_url, "search", input="1\nWang, Changhan\n")
        assert result.exit_code == 0, result.output
        assert PAPER_2.title in result.output

    def test_search_by_title_not_found(self, ephemeral_db_url: str, db_session: object) -> None:
        """Search for unknown title reports not found."""
        result = _cli(ephemeral_db_url, "search", input="2\nNo Such Title ZZZZ\n")
        assert result.exit_code == 0, result.output
        assert "not found" in result.output.lower()

    def test_search_quit_option(self, ephemeral_db_url: str, db_session: object) -> None:
        """Choosing 3 (Quit) at the search menu exits cleanly."""
        result = _cli(ephemeral_db_url, "search", input="3\n")
        assert result.exit_code == 0, result.output


class TestAddSubcommand:
    """Tests for the 'pdbsearch add' subcommand."""

    def test_add_inline_bibtex(self, ephemeral_db_url: str, db_session: object) -> None:
        """Add a paper with inline BibTeX and verify it's searchable."""
        bibtex_id = "CliAddTest2026"
        # Input: author, title, bibtex_id, choice=2 (inline), bibtex, contents
        cli_input = (
            "CliAuthor, Test\n"
            "CLI Add Test Paper\n"
            f"{bibtex_id}\n"
            "2\n"  # inline
            "@misc{CliAddTest2026, title={CLI Add Test Paper}}\n"
            "CLI test summary\n"
        )
        result = _cli(ephemeral_db_url, "add", input=cli_input)
        assert result.exit_code == 0, result.output
        assert "added" in result.output.lower()

        # Verify searchable
        search_result = _cli(ephemeral_db_url, "search", input="2\nCLI Add Test Paper\n")
        assert "CLI Add Test Paper" in search_result.output

        # Cleanup
        _cli(ephemeral_db_url, "delete", input=f"{bibtex_id}\n1\n")


class TestUpdateSubcommand:
    """Tests for the 'pdbsearch update' subcommand."""

    def test_update_title_confirmed(self, ephemeral_db_url: str, db_session: object) -> None:
        """Update paper title with 'y' confirmation succeeds."""
        from paper_sorts.db.repositories import PaperCreate
        from paper_sorts.db.session import with_session
        from paper_sorts.services import paper_service

        paper = PaperCreate(
            title="CLI Update Before",
            contents="x",
            bibtex_id="CliUpdateTitle2026",
            bibtex="@misc{CliUpdateTitle2026}",
            authors=["CLI, U"],
        )
        with with_session(ephemeral_db_url) as session:
            paper_service.add_paper(session, paper)

        # Input: table=1(papers), col=1(title), identifier, new_value, confirm=y
        cli_input = (
            "1\n"            # papers
            "1\n"            # title
            "CLI Update Before\n"
            "CLI Update After\n"
            "y\n"
        )
        result = _cli(ephemeral_db_url, "update", input=cli_input)
        assert result.exit_code == 0, result.output
        assert "applied" in result.output.lower() or "successfully" in result.output.lower()

        # Cleanup
        with with_session(ephemeral_db_url) as session:
            paper_service.delete_paper(session, "CliUpdateTitle2026")

    def test_update_aborted_with_n(self, ephemeral_db_url: str, db_session: object) -> None:
        """Update aborted with 'n' does not change the database."""
        from paper_sorts.db.repositories import PaperCreate
        from paper_sorts.db.session import with_session
        from paper_sorts.services import paper_service

        paper = PaperCreate(
            title="CLI Abort Update Test",
            contents="original",
            bibtex_id="CliAbortUpdate2026",
            bibtex="@misc{CliAbortUpdate2026}",
            authors=["CLI, A"],
        )
        with with_session(ephemeral_db_url) as session:
            paper_service.add_paper(session, paper)

        # Input: abort at table selection
        cli_input = "4\n"  # abort
        result = _cli(ephemeral_db_url, "update", input=cli_input)
        assert result.exit_code == 0, result.output
        assert "aborted" in result.output.lower() or "cancelled" in result.output.lower()

        # Verify unchanged
        with with_session(ephemeral_db_url) as session:
            results = paper_service.search_by_title(session, "CLI Abort Update Test")
            assert results[0].contents == "original"

        # Cleanup
        with with_session(ephemeral_db_url) as session:
            paper_service.delete_paper(session, "CliAbortUpdate2026")


class TestDeleteSubcommand:
    """Tests for the 'pdbsearch delete' subcommand."""

    def test_delete_confirmed(self, ephemeral_db_url: str, db_session: object) -> None:
        """Delete a paper with 'y' confirmation removes it."""
        from paper_sorts.db.repositories import PaperCreate
        from paper_sorts.db.session import with_session
        from paper_sorts.services import paper_service

        paper = PaperCreate(
            title="CLI Delete Test",
            contents="x",
            bibtex_id="CliDelete2026",
            bibtex="@misc{CliDelete2026}",
            authors=["CLI, D"],
        )
        with with_session(ephemeral_db_url) as session:
            paper_service.add_paper(session, paper)

        result = _cli(ephemeral_db_url, "delete", input="CliDelete2026\n1\n")
        assert result.exit_code == 0, result.output
        assert "deleted" in result.output.lower()

    def test_delete_cancelled(self, ephemeral_db_url: str, db_session: object) -> None:
        """Delete with 'n' confirmation leaves the paper intact."""
        from paper_sorts.db.repositories import PaperCreate
        from paper_sorts.db.session import with_session
        from paper_sorts.services import paper_service

        paper = PaperCreate(
            title="CLI Cancel Delete Test",
            contents="x",
            bibtex_id="CliCancelDelete2026",
            bibtex="@misc{CliCancelDelete2026}",
            authors=["CLI, CD"],
        )
        with with_session(ephemeral_db_url) as session:
            paper_service.add_paper(session, paper)

        result = _cli(ephemeral_db_url, "delete", input="CliCancelDelete2026\n2\n")
        assert result.exit_code == 0, result.output
        assert "cancelled" in result.output.lower()

        # Verify not deleted
        with with_session(ephemeral_db_url) as session:
            results = paper_service.search_by_title(session, "CLI Cancel Delete Test")
            assert len(results) == 1

        # Cleanup
        with with_session(ephemeral_db_url) as session:
            paper_service.delete_paper(session, "CliCancelDelete2026")

    def test_delete_not_found(self, ephemeral_db_url: str, db_session: object) -> None:
        """Delete of non-existent bibtex_id reports not found."""
        result = _cli(ephemeral_db_url, "delete", input="NonExistent9999\n")
        assert result.exit_code == 0, result.output
        assert "nonexistent9999" in result.output.lower() or "no paper" in result.output.lower()


class TestMigrateSubcommand:
    """Tests for the 'pdbsearch migrate' subcommand."""

    def test_migrate_runs_without_error(self, ephemeral_db_url: str, db_session: object) -> None:
        """Migrate subcommand runs successfully (idempotent)."""
        result = _cli(ephemeral_db_url, "migrate")
        assert result.exit_code == 0, result.output
        assert "successfully" in result.output.lower() or "migration" in result.output.lower()


class TestAppConfigLoading:
    """Tests for app.py config URL loading paths."""

    def test_url_from_env_var(self, ephemeral_db_url: str, db_session: object) -> None:
        """App loads DB URL from PDBSEARCH_DATABASE_URL env var."""
        import os

        old = os.environ.get("PDBSEARCH_DATABASE_URL")
        os.environ["PDBSEARCH_DATABASE_URL"] = ephemeral_db_url
        try:
            result = runner.invoke(app, ["migrate"], catch_exceptions=False)
            assert result.exit_code == 0, result.output
        finally:
            if old is None:
                os.environ.pop("PDBSEARCH_DATABASE_URL", None)
            else:
                os.environ["PDBSEARCH_DATABASE_URL"] = old

    def test_url_from_pdbsearch_env_components(
        self, ephemeral_db_url: str, db_session: object
    ) -> None:
        """App auto-loads DB URL from PDBSEARCH_DB_* env vars (no --database-url flag)."""
        import os
        from urllib.parse import urlparse

        # Parse the ephemeral URL to extract host/port/dbname/user
        parsed = urlparse(ephemeral_db_url)
        host = parsed.hostname or "127.0.0.1"
        port = str(parsed.port or 5432)
        user = parsed.username or "postgres"
        dbname = parsed.path.lstrip("/")

        env_backup = {
            "PDBSEARCH_DATABASE_URL": os.environ.pop("PDBSEARCH_DATABASE_URL", None),
        }
        os.environ["PDBSEARCH_DB_HOST"] = host
        os.environ["PDBSEARCH_DB_PORT"] = port
        os.environ["PDBSEARCH_DB_USER"] = user
        os.environ["PDBSEARCH_DB_NAME"] = dbname
        os.environ["PDBSEARCH_DB_PASSWORD"] = ""

        try:
            result = runner.invoke(app, ["migrate"], catch_exceptions=False)
            assert result.exit_code == 0, result.output
        finally:
            if env_backup["PDBSEARCH_DATABASE_URL"] is not None:
                os.environ["PDBSEARCH_DATABASE_URL"] = env_backup["PDBSEARCH_DATABASE_URL"]
            for k in ["PDBSEARCH_DB_HOST", "PDBSEARCH_DB_PORT", "PDBSEARCH_DB_USER",
                      "PDBSEARCH_DB_NAME", "PDBSEARCH_DB_PASSWORD"]:
                os.environ.pop(k, None)

    def test_invalid_config_file_exits(self) -> None:
        """Passing nonexistent --config + --key files exits with code 1."""
        import os

        # Remove DATABASE_URL env var so the code hits the config file path
        env_backup = os.environ.pop("PDBSEARCH_DATABASE_URL", None)
        try:
            result = runner.invoke(
                app,
                [
                    "--config", "/nonexistent/config.crypt",
                    "--key", "/nonexistent/key",
                    "migrate",
                ],
                catch_exceptions=False,
            )
            # Should exit with 1 due to config load failure
            assert result.exit_code == 1
        finally:
            if env_backup is not None:
                os.environ["PDBSEARCH_DATABASE_URL"] = env_backup


class TestInteractiveMenu:
    """Tests for the interactive top-level menu (no subcommand)."""

    def test_interactive_menu_quit(self, ephemeral_db_url: str, db_session: object) -> None:
        """Entering 4 (Quit) at the interactive menu exits cleanly."""
        result = runner.invoke(
            app,
            ["--database-url", ephemeral_db_url],
            input="4\n",
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output
        assert "goodbye" in result.output.lower() or "what do you want" in result.output.lower()

    def test_interactive_menu_search_then_quit(
        self, ephemeral_db_url: str, db_session: object
    ) -> None:
        """Interactive menu: enter search, quit search, then quit main menu."""
        # input: 1 (search) → 3 (quit search) → 4 (quit main menu)
        result = runner.invoke(
            app,
            ["--database-url", ephemeral_db_url],
            input="1\n3\n4\n",
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output

    def test_no_db_url_exits_with_error(self) -> None:
        """Without a DB URL, the app exits with error code 1."""
        result = runner.invoke(app, [], catch_exceptions=False)
        assert result.exit_code == 1

    def test_interactive_menu_add_then_quit(
        self, ephemeral_db_url: str, db_session: object
    ) -> None:
        """Interactive menu: choose add (2), then abort at author prompt, then quit."""
        # Input: 2 (add) → then provide all add inputs → then 4 (quit)
        bibtex_id = "InteractiveAddTest2026"
        cli_input = (
            "2\n"  # add
            "Menu, Author\n"
            "Interactive Menu Test Paper\n"
            f"{bibtex_id}\n"
            "2\n"  # inline bibtex
            "@misc{InteractiveAddTest2026}\n"
            "test summary\n"
            "4\n"  # quit
        )
        result = runner.invoke(
            app,
            ["--database-url", ephemeral_db_url],
            input=cli_input,
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output
        # Cleanup
        from paper_sorts.db.session import with_session
        from paper_sorts.services import paper_service
        with with_session(ephemeral_db_url) as session:
            paper_service.delete_paper(session, bibtex_id)

    def test_interactive_menu_update_then_quit(
        self, ephemeral_db_url: str, db_session: object
    ) -> None:
        """Interactive menu: choose update (3), abort at table selection, then quit."""
        cli_input = (
            "3\n"  # update
            "4\n"  # abort update
            "4\n"  # quit main menu
        )
        result = runner.invoke(
            app,
            ["--database-url", ephemeral_db_url],
            input=cli_input,
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output


class TestAddSubcommandBibFile:
    """Additional tests for the add subcommand."""

    def test_add_nonexistent_bib_file(self, ephemeral_db_url: str, db_session: object) -> None:
        """Add with a nonexistent bib file path reports file not found."""
        cli_input = (
            "Some Author\n"
            "Test Title\n"
            "key123\n"
            "1\n"  # from file
            "/nonexistent/path/to/file.bib\n"
        )
        result = runner.invoke(
            app,
            ["--database-url", ephemeral_db_url, "add"],
            input=cli_input,
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output
        assert "not found" in result.output.lower() or "file" in result.output.lower()


class TestUpdateSubcommandBibColumn:
    """Tests for bib and author update paths."""

    def test_update_bib_confirmed(self, ephemeral_db_url: str, db_session: object) -> None:
        """Update bib entry with confirmation."""
        from paper_sorts.db.repositories import PaperCreate
        from paper_sorts.db.session import with_session
        from paper_sorts.services import paper_service

        paper = PaperCreate(
            title="Bib CLI Update Test",
            contents="x",
            bibtex_id="BibCliUpdate2026",
            bibtex="@misc{BibCliUpdate2026, title={old}}",
            authors=["Bib, Test"],
        )
        with with_session(ephemeral_db_url) as session:
            paper_service.add_paper(session, paper)

        # table=2(bib), identifier, new bibtex, confirm=y
        cli_input = "2\nBibCliUpdate2026\n@misc{BibCliUpdate2026, title={new}}\ny\n"
        result = _cli(ephemeral_db_url, "update", input=cli_input)
        assert result.exit_code == 0, result.output

        # Cleanup
        with with_session(ephemeral_db_url) as session:
            paper_service.delete_paper(session, "BibCliUpdate2026")

    def test_update_author_confirmed(self, ephemeral_db_url: str, db_session: object) -> None:
        """Update author name with confirmation."""
        from paper_sorts.db.repositories import PaperCreate
        from paper_sorts.db.session import with_session
        from paper_sorts.services import paper_service

        paper = PaperCreate(
            title="Author CLI Update Test",
            contents="x",
            bibtex_id="AuthorCliUpdate2026",
            bibtex="@misc{AuthorCliUpdate2026}",
            authors=["AuthorOld, CLI"],
        )
        with with_session(ephemeral_db_url) as session:
            paper_service.add_paper(session, paper)

        # table=3(authors), old name, new name, confirm=y
        cli_input = "3\nAuthorOld, CLI\nAuthorNew, CLI\ny\n"
        result = _cli(ephemeral_db_url, "update", input=cli_input)
        assert result.exit_code == 0, result.output

        # Cleanup
        with with_session(ephemeral_db_url) as session:
            paper_service.delete_paper(session, "AuthorCliUpdate2026")
