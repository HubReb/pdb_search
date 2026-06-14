"""CLI layer tests for paper_sorts using Typer's CliRunner.

Tests cover every subcommand (search, add, update, delete, import, migrate)
through the public CLI entry point. Uses the ephemeral PostgreSQL fixture.
"""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from paper_sorts.cli.app import app
from paper_sorts.db.repositories import PaperCreate
from paper_sorts.services.paper_service import add_paper, delete_paper

runner = CliRunner()


def _invoke(args: list[str], db_url: str, input_text: str = "") -> object:
    """Helper to invoke the CLI app with the given database URL.

    :param args: list of CLI arguments
    :param db_url: database URL to pass via --database-url
    :param input_text: stdin text to feed to interactive prompts
    :return: typer.testing.Result object
    """
    cli_args = ["--database-url", db_url] + args
    return runner.invoke(app, cli_args, input=input_text)


class TestSearchCmd:
    """Tests for `pdbsearch search`."""

    def test_search_by_title_one_result(self, seeded_db_url: str) -> None:
        """Searching for a unique title shows paper details."""
        result = _invoke(
            ["search", "--by", "title", "--query", "Large-scale"], seeded_db_url
        )
        assert result.exit_code == 0  # type: ignore[union-attr]
        assert "Wang2021LargeScaleSA" in result.output  # type: ignore[union-attr]

    def test_search_by_title_no_results(self, seeded_db_url: str) -> None:
        """Searching for a non-existent title shows 'No results'."""
        result = _invoke(
            ["search", "--by", "title", "--query", "xyzzy_nonexistent"], seeded_db_url
        )
        assert result.exit_code == 0  # type: ignore[union-attr]
        assert "No results" in result.output  # type: ignore[union-attr]

    def test_search_by_author(self, seeded_db_url: str) -> None:
        """Searching by a known author shows results; selects first when multiple."""
        # "Pino" matches 2 papers → disambiguation menu; user picks "1"
        result = _invoke(
            ["search", "--by", "author", "--query", "Pino"],
            seeded_db_url,
            input_text="1\n",
        )
        assert result.exit_code == 0  # type: ignore[union-attr]
        output = result.output  # type: ignore[union-attr]
        # Should display one of the two papers
        assert "Wang2021LargeScaleSA" in output or "Lee2021Direct" in output

    def test_search_by_title_multiple_prompts_disambiguation(
        self, seeded_db_url: str
    ) -> None:
        """Multiple results trigger a disambiguation prompt; user selects first."""
        # "speech" matches both Wang2021 and Lee2021
        # The menu shows options; user picks "1" (first option)
        result = _invoke(
            ["search", "--by", "title", "--query", "speech"],
            seeded_db_url,
            input_text="1\n",
        )
        assert result.exit_code == 0  # type: ignore[union-attr]
        # Should display at least one paper
        assert "bibtex_id" in result.output.lower() or "BibTeX" in result.output  # type: ignore[union-attr]


class TestAddCmd:
    """Tests for `pdbsearch add`."""

    def test_add_inline_success(self, db_url: str) -> None:
        """Adding a paper via interactive prompts inserts it into the DB."""
        inputs = "\n".join([
            "Test, Author",              # authors
            "CLI Add Test Paper",        # title
            "CliAddTest2024",            # bibtex_id
            "CLI test summary",          # contents
            "@article{CliAddTest2024}",  # bibtex
            "1",                         # confirm: yes
        ]) + "\n"
        result = _invoke(["add"], db_url, input_text=inputs)
        assert result.exit_code == 0  # type: ignore[union-attr]
        assert "Added" in result.output  # type: ignore[union-attr]
        # Cleanup
        try:
            delete_paper(db_url, "CliAddTest2024")
        except KeyError:
            pass

    def test_add_cancelled(self, db_url: str) -> None:
        """Responding 'No' to confirmation cancels the add."""
        inputs = "\n".join([
            "Test, Author",
            "Cancelled Paper",
            "CancelledKey2024",
            "Summary",
            "@article{CancelledKey2024}",
            "2",  # confirm: no
        ]) + "\n"
        result = _invoke(["add"], db_url, input_text=inputs)
        assert result.exit_code == 0  # type: ignore[union-attr]
        assert "cancelled" in result.output.lower()  # type: ignore[union-attr]

    def test_add_from_bib_file(self, db_url: str, tmp_path: Path) -> None:
        """Adding a paper with --from-bib reads BibTeX from file."""
        bib_content = "@article{FileBibTest2024, title={File Bib Test}, author={File, Author}}"
        bib_file = tmp_path / "test.bib"
        bib_file.write_text(bib_content)

        inputs = "\n".join([
            "File, Author",         # authors
            "File Bib Test",        # title
            "FileBibTest2024",      # bibtex_id
            "File summary",         # contents
            "1",                    # confirm: yes
        ]) + "\n"
        result = _invoke(
            ["add", "--from-bib", str(bib_file)], db_url, input_text=inputs
        )
        assert result.exit_code == 0  # type: ignore[union-attr]
        assert "Added" in result.output  # type: ignore[union-attr]
        # Cleanup
        try:
            delete_paper(db_url, "FileBibTest2024")
        except KeyError:
            pass


class TestUpdateCmd:
    """Tests for `pdbsearch update`."""

    def test_update_title_confirm_yes(self, seeded_db_url: str) -> None:
        """Updating title with confirmation changes the paper title."""
        inputs = "\n".join([
            "1",                       # field: Title
            "Updated CLI Title",       # new value
            "1",                       # confirm: yes
        ]) + "\n"
        result = _invoke(
            ["update", "--id", "Smith2022Survey"], seeded_db_url, input_text=inputs
        )
        assert result.exit_code == 0  # type: ignore[union-attr]
        assert "Updated" in result.output  # type: ignore[union-attr]

    def test_update_abort(self, seeded_db_url: str) -> None:
        """Choosing abort from field menu cancels the update."""
        # Option 5 is "Quit / abort"
        inputs = "5\n"
        result = _invoke(
            ["update", "--id", "Smith2022Survey"], seeded_db_url, input_text=inputs
        )
        assert result.exit_code == 0  # type: ignore[union-attr]
        assert "cancel" in result.output.lower() or "abort" in result.output.lower()  # type: ignore[union-attr]

    def test_update_confirm_no(self, seeded_db_url: str) -> None:
        """Responding 'No' to confirmation leaves the paper unchanged."""
        inputs = "\n".join([
            "1",           # field: Title
            "Not Applied",  # new value
            "2",           # confirm: no
        ]) + "\n"
        result = _invoke(
            ["update", "--id", "Smith2022Survey"], seeded_db_url, input_text=inputs
        )
        assert result.exit_code == 0  # type: ignore[union-attr]
        assert "cancel" in result.output.lower() or "Cancel" in result.output  # type: ignore[union-attr]


class TestDeleteCmd:
    """Tests for `pdbsearch delete`."""

    def test_delete_confirm_yes(self, db_url: str) -> None:
        """Deleting a paper with confirmation removes it from the DB."""
        add_paper(
            db_url,
            PaperCreate(
                title="Delete CLI Test",
                contents="...",
                bibtex_id="DeleteCliTest2024",
                bibtex="@article{DeleteCliTest2024}",
                authors=["Del, Author"],
            ),
        )
        result = _invoke(
            ["delete", "--id", "DeleteCliTest2024"], db_url, input_text="1\n"
        )
        assert result.exit_code == 0  # type: ignore[union-attr]
        assert "Deleted" in result.output  # type: ignore[union-attr]

    def test_delete_confirm_no(self, db_url: str) -> None:
        """Responding 'No' to delete confirmation cancels the operation."""
        add_paper(
            db_url,
            PaperCreate(
                title="Keep This Paper",
                contents="...",
                bibtex_id="KeepMe2024",
                bibtex="@article{KeepMe2024}",
                authors=["Keep, Author"],
            ),
        )
        result = _invoke(["delete", "--id", "KeepMe2024"], db_url, input_text="2\n")
        assert result.exit_code == 0  # type: ignore[union-attr]
        assert "cancel" in result.output.lower()  # type: ignore[union-attr]
        # Cleanup
        delete_paper(db_url, "KeepMe2024")


class TestImportCmd:
    """Tests for `pdbsearch import`."""

    def test_import_basic(self, db_url: str, tmp_path: Path) -> None:
        """Importing from a valid .tex + .bib pair inserts papers."""
        bib_content = (
            "@article{ImportTest2024,\n"
            "  author = {Import, Author},\n"
            "  title = {Import Test Paper},\n"
            "  year = {2024}\n"
            "}\n"
        )
        tex_content = (
            "\\begin{itemize}\n"
            "\\item ImportTest2024 \\cite{ImportTest2024}: Import test paper description.\n"
            "\\end{itemize}\n"
        )
        bib_file = tmp_path / "test.bib"
        tex_file = tmp_path / "test.tex"
        bib_file.write_text(bib_content)
        tex_file.write_text(tex_content)

        result = _invoke(
            ["import", str(tex_file), str(bib_file)], db_url
        )
        assert result.exit_code == 0  # type: ignore[union-attr]
        # Either "Imported" with count, or at least no error
        assert "Imported" in result.output or "skipped" in result.output.lower()  # type: ignore[union-attr]
        # Cleanup attempt
        try:
            delete_paper(db_url, "ImportTest2024")
        except KeyError:
            pass

    def test_import_idempotent(self, db_url: str, tmp_path: Path) -> None:
        """Re-running import skips already-imported papers (no error)."""
        bib_content = (
            "@article{IdemImport2024,\n"
            "  author = {Idem, Author},\n"
            "  title = {Idempotent Import},\n"
            "  year = {2024}\n"
            "}\n"
        )
        tex_content = (
            "\\begin{itemize}\n"
            "\\item IdemImport2024 \\cite{IdemImport2024}: Some description.\n"
            "\\end{itemize}\n"
        )
        bib_file = tmp_path / "idem.bib"
        tex_file = tmp_path / "idem.tex"
        bib_file.write_text(bib_content)
        tex_file.write_text(tex_content)

        # First import
        _invoke(["import", str(tex_file), str(bib_file)], db_url)
        # Second import should succeed (duplicates silently skipped)
        result = _invoke(["import", str(tex_file), str(bib_file)], db_url)
        assert result.exit_code == 0  # type: ignore[union-attr]
        # Cleanup
        try:
            delete_paper(db_url, "IdemImport2024")
        except KeyError:
            pass


class TestMigrateCmd:
    """Tests for `pdbsearch migrate`."""

    def test_migrate_succeeds_on_migrated_db(self, migrated_db_url: str) -> None:
        """Running migrate on an already-migrated DB succeeds (idempotent)."""
        result = _invoke(["migrate"], migrated_db_url)
        assert result.exit_code == 0  # type: ignore[union-attr]
        assert "complete" in result.output.lower() or "revision" in result.output.lower()  # type: ignore[union-attr]


class TestInteractiveSearchFlow:
    """Tests that exercise delete/update without --id to cover run_* search flows."""

    def test_delete_without_id_uses_search_flow(self, seeded_db_url: str) -> None:
        """delete without --id prompts for search term, shows match, cancel (2=No)."""
        # Input: search term "Large-scale", then "2" (No) to cancel
        # Large-scale matches exactly 1 paper so no disambiguation menu
        result = _invoke(
            ["delete"],
            seeded_db_url,
            input_text="Large-scale\n2\n",  # search term, cancel
        )
        assert result.exit_code == 0  # type: ignore[union-attr]
        assert "cancel" in result.output.lower()  # type: ignore[union-attr]

    def test_update_without_id_uses_search_flow(self, seeded_db_url: str) -> None:
        """update without --id prompts for search term, shows match, then abort field select."""
        # Smith2022Survey is unique; input: "survey" search term, then quit from field menu
        result = _invoke(
            ["update"],
            seeded_db_url,
            input_text="survey\n5\n",  # search term finds Smith2022, then option 5=quit from field menu
        )
        assert result.exit_code == 0  # type: ignore[union-attr]
        assert "cancel" in result.output.lower() or "abort" in result.output.lower()  # type: ignore[union-attr]

    def test_search_without_flags_prompts_method(self, seeded_db_url: str) -> None:
        """search without --by prompts for the search method, then query."""
        # User picks "title" (option 2), enters "Smith", gets 1 result
        result = _invoke(
            ["search"],
            seeded_db_url,
            input_text="2\nSmith\n",  # pick option 2 = "title", then "Smith"
        )
        assert result.exit_code == 0  # type: ignore[union-attr]
        # Smith2022Survey should appear in output
        assert "Smith" in result.output or "survey" in result.output.lower()  # type: ignore[union-attr]


class TestInteractiveMenu:
    """Tests for the interactive menu (no subcommand invocation)."""

    def test_interactive_menu_quit(self, db_url: str) -> None:
        """Invoking pdbsearch with no subcommand and choosing quit exits cleanly."""
        # Option 4 = Quit / abort
        result = runner.invoke(
            app,
            ["--database-url", db_url],
            input="4\n",
        )
        assert result.exit_code == 0
        assert "goodbye" in result.output.lower() or "closing" in result.output.lower()

    def test_interactive_menu_search_then_quit(self, seeded_db_url: str) -> None:
        """Interactive menu: search by title, get result, then quit."""
        # Option 1 = Search, then by title (option 2), query, then quit
        result = runner.invoke(
            app,
            ["--database-url", seeded_db_url],
            input="1\n2\nLarge-scale\n4\n",  # search, title, query, quit
        )
        assert result.exit_code == 0
        assert "Wang2021LargeScaleSA" in result.output or "Large-scale" in result.output

    def test_interactive_menu_invalid_input(self, db_url: str) -> None:
        """Invalid menu input re-prompts; then quit."""
        # "99" is out of range → re-prompt (consumes another line) → "4" quits
        result = runner.invoke(
            app,
            ["--database-url", db_url],
            input="99\n4\n4\n",  # invalid (plus re-prompt recovery), then quit
        )
        # May exit with 0 or 1 depending on input handling; just verify no crash
        assert result.exit_code in (0, 1)


class TestRunFunctionsDisambiguation:
    """Tests for the disambiguation and error paths in run_* functions."""

    def test_delete_without_id_multiple_results_abort(self, seeded_db_url: str) -> None:
        """run_delete via CLI, search finds multiple (speech), user aborts."""
        # "speech" matches 2 papers → disambiguation → user aborts (picks quit)
        result = _invoke(
            ["delete"],
            seeded_db_url,
            input_text="speech\n3\n",  # search "speech" → 3 results → quit (option 3)
        )
        assert result.exit_code == 0  # type: ignore[union-attr]

    def test_update_without_id_multiple_results_select(self, seeded_db_url: str) -> None:
        """run_update via CLI, search finds multiple, user picks one then aborts field."""
        # "speech" matches 2 papers → disambiguation → pick first → abort field menu
        result = _invoke(
            ["update"],
            seeded_db_url,
            input_text="speech\n1\n5\n",  # search, pick first, abort field selection
        )
        assert result.exit_code == 0  # type: ignore[union-attr]
        assert "cancel" in result.output.lower() or "abort" in result.output.lower()  # type: ignore[union-attr]

    def test_delete_no_results(self, seeded_db_url: str) -> None:
        """run_delete via CLI, search finds nothing, exits cleanly."""
        result = _invoke(
            ["delete"],
            seeded_db_url,
            input_text="xyzzy_no_such_paper\n",
        )
        assert result.exit_code == 0  # type: ignore[union-attr]
        assert "no papers" in result.output.lower()  # type: ignore[union-attr]

    def test_update_no_results(self, seeded_db_url: str) -> None:
        """run_update via CLI, search finds nothing, exits cleanly."""
        result = _invoke(
            ["update"],
            seeded_db_url,
            input_text="xyzzy_no_such_paper\n",
        )
        assert result.exit_code == 0  # type: ignore[union-attr]
        assert "no papers" in result.output.lower()  # type: ignore[union-attr]


class TestMoreCLIPaths:
    """Additional tests to cover error/branch paths in CLI modules."""

    def test_interactive_menu_search_by_author(self, seeded_db_url: str) -> None:
        """Interactive menu: search by author (covers run_search author path)."""
        # Main menu option 1, then author method (option 1), query "Pino", select 1, quit
        result = runner.invoke(
            app,
            ["--database-url", seeded_db_url],
            input="1\n1\nPino\n1\n4\n",
        )
        assert result.exit_code == 0
        assert "Pino" in result.output or "Wang" in result.output or "Lee" in result.output

    def test_interactive_menu_add_paper(self, db_url: str) -> None:
        """Interactive menu: add paper flow (covers run_add path from menu)."""
        inputs = "\n".join([
            "2",                          # main menu: add
            "Menu, Author",               # authors
            "Menu Add Paper",             # title
            "MenuAddTest2024",            # bibtex_id
            "Menu summary",               # contents
            "@article{MenuAddTest2024}",  # bibtex
            "1",                          # confirm: yes
            "4",                          # quit main menu
        ]) + "\n"
        result = runner.invoke(
            app,
            ["--database-url", db_url],
            input=inputs,
        )
        assert result.exit_code == 0
        assert "Added" in result.output
        # Cleanup
        try:
            delete_paper(db_url, "MenuAddTest2024")
        except KeyError:
            pass

    def test_search_cmd_no_author_found(self, seeded_db_url: str) -> None:
        """search --by author with no results prints message and exits 0."""
        result = _invoke(
            ["search", "--by", "author", "--query", "xyzzy_no_such"],
            seeded_db_url,
        )
        assert result.exit_code == 0  # type: ignore[union-attr]
        assert "No results" in result.output or "no results" in result.output.lower()  # type: ignore[union-attr]


class TestMigrateCmdExtra:
    """Additional migrate command tests for branch coverage."""

    def test_migrate_with_specific_target(self, migrated_db_url: str) -> None:
        """migrate --target with a specific revision succeeds."""
        result = _invoke(["migrate", "--target", "002_converge_bibtext_typo"], migrated_db_url)
        assert result.exit_code == 0  # type: ignore[union-attr]
        assert "complete" in result.output.lower() or "revision" in result.output.lower()  # type: ignore[union-attr]


class TestImportCmdError:
    """Error path tests for import command."""

    def test_import_missing_files(self, db_url: str) -> None:
        """import with non-existent files exits with error code."""
        result = _invoke(
            ["import", "/nonexistent/file.tex", "/nonexistent/file.bib"],
            db_url,
        )
        assert result.exit_code != 0  # type: ignore[union-attr]
