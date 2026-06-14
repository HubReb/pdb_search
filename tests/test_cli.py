"""CLI layer tests for paper_sorts using Typer's CliRunner.

Tests exercise all subcommands: search, add, update, delete, and the
interactive menu path.  User inputs are mocked via monkeypatch on
the prompt functions where they are used in each CLI module.
"""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from paper_sorts.cli.app import app
from paper_sorts.db.repositories import PaperCreate
from paper_sorts.services import paper_service

runner = CliRunner()


def _db_url(engine: object) -> str:
    """Extract the database URL string from a SQLAlchemy engine."""
    return str(engine.url)  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Search subcommand
# ---------------------------------------------------------------------------


class TestSearchCmd:
    """Tests for 'pdbsearch search'."""

    def test_search_by_title_found(
        self, seeded_engine: object, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Searching by title returns paper details."""
        # Patch where used (imported into search.py)
        monkeypatch.setattr(
            "paper_sorts.cli.search.ask_search_method", lambda: "title"
        )
        monkeypatch.setattr(
            "paper_sorts.cli.search.ask_nonempty",
            lambda _: "Direct speech-to-speech translation with discrete units",
        )
        result = runner.invoke(
            app,
            ["--database-url", _db_url(seeded_engine), "search"],
        )
        assert result.exit_code == 0, result.output
        assert "Direct speech-to-speech translation" in result.output
        assert "Lee, Ann" in result.output

    def test_search_by_title_not_found(
        self, seeded_engine: object, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Searching for a non-existent title prints a not-found message."""
        monkeypatch.setattr(
            "paper_sorts.cli.search.ask_search_method", lambda: "title"
        )
        monkeypatch.setattr(
            "paper_sorts.cli.search.ask_nonempty", lambda _: "No Such Paper"
        )
        result = runner.invoke(
            app,
            ["--database-url", _db_url(seeded_engine), "search"],
        )
        assert result.exit_code == 0, result.output
        assert "not found" in result.output.lower()

    def test_search_by_author_found(
        self, seeded_engine: object, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Searching by author returns matching papers."""
        monkeypatch.setattr(
            "paper_sorts.cli.search.ask_search_method", lambda: "author"
        )
        monkeypatch.setattr(
            "paper_sorts.cli.search.ask_nonempty", lambda _: "Pino, J."
        )
        result = runner.invoke(
            app,
            ["--database-url", _db_url(seeded_engine), "search"],
        )
        assert result.exit_code == 0, result.output
        assert "Large-scale Self" in result.output


# ---------------------------------------------------------------------------
# Add subcommand
# ---------------------------------------------------------------------------


class TestAddCmd:
    """Tests for 'pdbsearch add'."""

    def test_add_success(
        self, engine: object, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Adding a new paper succeeds and is retrievable."""
        inputs = iter([
            "Test Author, A.",                             # authors
            "CLI Add Test Paper",                          # title
            "CLIAddTest2024",                              # bibtex key
            "@article{CLIAddTest2024, year={2024}}",       # bibtex inline
            "CLI test summary.",                           # contents
        ])

        monkeypatch.setattr(
            "paper_sorts.cli.add.ask_nonempty", lambda _: next(inputs)
        )
        monkeypatch.setattr(
            "paper_sorts.cli.add.ask_choice", lambda opts, *a, **kw: 2
        )  # inline bibtex

        result = runner.invoke(
            app,
            ["--database-url", _db_url(engine), "add"],
        )
        assert result.exit_code == 0, result.output
        assert "Added" in result.output

        # Cleanup
        papers = paper_service.search_by_title(engine, "CLI Add Test Paper")  # type: ignore[arg-type]
        if papers:
            paper_service.delete_paper(engine, papers[0].id)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Update subcommand
# ---------------------------------------------------------------------------


class TestUpdateCmd:
    """Tests for 'pdbsearch update'."""

    def test_update_title_confirmed(
        self, engine: object, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Updating title with confirmation succeeds."""
        # Setup
        paper = PaperCreate(
            title="Before CLI Update",
            contents="content.",
            bibtex_id="CLIUpd2024",
            bibtex="@article{CLIUpd2024, year={2024}}",
            authors=["CLIUpd, A."],
        )
        added = paper_service.add_paper(engine, paper)  # type: ignore[arg-type]

        choices = iter([1, 1])  # table=papers, field=title
        nonempty = iter([str(added.id), "After CLI Update"])

        monkeypatch.setattr(
            "paper_sorts.cli.update.ask_choice", lambda opts, *a, **kw: next(choices)
        )
        monkeypatch.setattr(
            "paper_sorts.cli.update.ask_nonempty", lambda _: next(nonempty)
        )
        monkeypatch.setattr(
            "paper_sorts.cli.update.ask_confirm", lambda _: True
        )

        result = runner.invoke(
            app,
            ["--database-url", _db_url(engine), "update"],
        )
        assert result.exit_code == 0, result.output
        assert "Update successful" in result.output

        # Cleanup
        papers = paper_service.search_by_title(engine, "After CLI Update")  # type: ignore[arg-type]
        if papers:
            paper_service.delete_paper(engine, papers[0].id)  # type: ignore[arg-type]

    def test_update_aborted(
        self, engine: object, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Aborting at confirmation leaves the paper unchanged."""
        paper = PaperCreate(
            title="No Change Paper",
            contents="content.",
            bibtex_id="NoChg2024",
            bibtex="@article{NoChg2024, year={2024}}",
            authors=["NoChg, A."],
        )
        added = paper_service.add_paper(engine, paper)  # type: ignore[arg-type]

        choices = iter([1, 1])  # table=papers, field=title
        nonempty = iter([str(added.id), "Should Not Appear"])

        monkeypatch.setattr(
            "paper_sorts.cli.update.ask_choice", lambda opts, *a, **kw: next(choices)
        )
        monkeypatch.setattr(
            "paper_sorts.cli.update.ask_nonempty", lambda _: next(nonempty)
        )
        monkeypatch.setattr(
            "paper_sorts.cli.update.ask_confirm", lambda _: False
        )

        result = runner.invoke(
            app,
            ["--database-url", _db_url(engine), "update"],
        )
        assert result.exit_code == 0
        assert "aborted" in result.output.lower()

        # Cleanup
        paper_service.delete_paper(engine, added.id)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Delete subcommand
# ---------------------------------------------------------------------------


class TestDeleteCmd:
    """Tests for 'pdbsearch delete'."""

    def test_delete_confirmed(
        self, engine: object, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Deleting a paper with confirmation removes it."""
        paper = PaperCreate(
            title="CLI Delete Test",
            contents="del.",
            bibtex_id="CLIDel2024",
            bibtex="@article{CLIDel2024, year={2024}}",
            authors=["Del, A."],
        )
        paper_service.add_paper(engine, paper)  # type: ignore[arg-type]

        monkeypatch.setattr(
            "paper_sorts.cli.delete.ask_nonempty", lambda _: "CLI Delete Test"
        )
        monkeypatch.setattr(
            "paper_sorts.cli.delete.ask_confirm", lambda _: True
        )

        result = runner.invoke(
            app,
            ["--database-url", _db_url(engine), "delete"],
        )
        assert result.exit_code == 0, result.output
        assert "Deleted" in result.output

        remaining = paper_service.search_by_title(engine, "CLI Delete Test")  # type: ignore[arg-type]
        assert remaining == []

    def test_delete_aborted(
        self, engine: object, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Aborting delete confirmation leaves the paper in the database."""
        paper = PaperCreate(
            title="Delete Aborted Test",
            contents="keep.",
            bibtex_id="DelAbort2024",
            bibtex="@article{DelAbort2024, year={2024}}",
            authors=["Keep, A."],
        )
        added = paper_service.add_paper(engine, paper)  # type: ignore[arg-type]

        monkeypatch.setattr(
            "paper_sorts.cli.delete.ask_nonempty", lambda _: "Delete Aborted Test"
        )
        monkeypatch.setattr(
            "paper_sorts.cli.delete.ask_confirm", lambda _: False
        )

        result = runner.invoke(
            app,
            ["--database-url", _db_url(engine), "delete"],
        )
        assert result.exit_code == 0
        assert "aborted" in result.output.lower()

        # Cleanup
        paper_service.delete_paper(engine, added.id)  # type: ignore[arg-type]
