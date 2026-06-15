"""End-to-end CLI tests via Typer's CliRunner against the ephemeral database."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from sqlalchemy import Engine
from typer.testing import CliRunner

from paper_sorts.cli import app as app_module
from paper_sorts.cli import prompts

runner = CliRunner()


def _feed(monkeypatch: pytest.MonkeyPatch, answers: list[str]) -> None:
    """Patch ``Prompt.ask`` to return scripted answers in order."""
    it: Iterator[str] = iter(answers)

    def fake_ask(*_args: object, **_kwargs: object) -> str:
        return next(it)

    monkeypatch.setattr(prompts.Prompt, "ask", staticmethod(fake_ask))


def test_help_lists_subcommands() -> None:
    result = runner.invoke(app_module.app, ["--help"])
    assert result.exit_code == 0
    for cmd in ("search", "add", "update", "delete", "import", "migrate"):
        assert cmd in result.output


def test_search_subcommand(
    monkeypatch: pytest.MonkeyPatch, seeded_engine: Engine, ephemeral_db_url: str
) -> None:
    # author search -> 'Wang, Changhan'
    _feed(monkeypatch, ["1", "Wang, Changhan"])
    result = runner.invoke(app_module.app, ["--database-url", ephemeral_db_url, "search"])
    assert result.exit_code == 0
    assert "Wang2021LargeScaleSA" in result.output


def test_add_subcommand(
    monkeypatch: pytest.MonkeyPatch, engine: Engine, ephemeral_db_url: str
) -> None:
    _feed(
        monkeypatch,
        [
            "Doe, Jane",  # authors
            "CLI Added Paper",  # title
            "Cli2026",  # bibtex key
            "2",  # bib via file? -> No (inline)
            "@a{Cli2026}",  # bib entry
            "A summary.",  # summary
        ],
    )
    result = runner.invoke(app_module.app, ["--database-url", ephemeral_db_url, "add"])
    assert result.exit_code == 0
    # Re-find it via a fresh search invocation.
    _feed(monkeypatch, ["2", "CLI Added Paper"])
    found = runner.invoke(app_module.app, ["--database-url", ephemeral_db_url, "search"])
    assert "Cli2026" in found.output


def test_delete_subcommand_with_confirm(
    monkeypatch: pytest.MonkeyPatch, seeded_engine: Engine, ephemeral_db_url: str
) -> None:
    _feed(
        monkeypatch, ["Large-scale Self- and Semi-Supervised learning for speech translation", "y"]
    )
    result = runner.invoke(app_module.app, ["--database-url", ephemeral_db_url, "delete"])
    assert result.exit_code == 0
    assert "deleted" in result.output.lower()


def test_no_subcommand_menu_quit(
    monkeypatch: pytest.MonkeyPatch, engine: Engine, ephemeral_db_url: str
) -> None:
    # The top menu has 4 options (1-3 + quit at 4); choosing quit closes.
    _feed(monkeypatch, ["4"])
    result = runner.invoke(app_module.app, ["--database-url", ephemeral_db_url])
    assert result.exit_code == 0
    assert "Closing connection" in result.output


def test_missing_database_url_errors() -> None:
    result = runner.invoke(app_module.app, ["search"], env={"PDBSEARCH_DATABASE_URL": ""})
    assert result.exit_code == 1
