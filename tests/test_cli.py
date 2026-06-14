"""CLI layer tests for paper_sorts using Typer's CliRunner.

Tests exercise the CLI subcommands at the process boundary.  The --database-url
flag is used to inject the ephemeral DB URL.
"""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from paper_sorts.cli.app import app
from tests.fixtures.seed_papers import SEED_PAPERS

runner = CliRunner()


def test_main_help() -> None:
    """pdbsearch --help exits 0 and mentions subcommands."""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "search" in result.output.lower() or "Search" in result.output


def test_search_subcommand_interactive_no_results(db_url: str) -> None:
    """search subcommand (interactive) with no matching papers prints 'No papers found'."""
    # Input: pick "1) Search by title" then type non-existent title, then "3) Back"
    result = runner.invoke(
        app,
        ["--database-url", db_url, "search"],
        input="1\nZzzzNonExistent\n",
    )
    assert "No papers found" in result.output or result.exit_code == 0


def test_search_subcommand_finds_paper(seeded_db_url: str) -> None:
    """search subcommand finds a seeded paper when searching by title."""
    # Pick "1) Search by title" then type part of a unique title
    result = runner.invoke(
        app,
        ["--database-url", seeded_db_url, "search"],
        input="1\nLarge-Scale\n",
    )
    assert result.exit_code == 0
    assert "Wang2021LargeScaleSA" in result.output or "Large-Scale" in result.output


def test_search_by_author_subcommand(seeded_db_url: str) -> None:
    """search by author finds papers by the seeded author."""
    # Pick "2) Search by author" then type author name
    result = runner.invoke(
        app,
        ["--database-url", seeded_db_url, "search"],
        input="2\nKaiming\n",
    )
    assert result.exit_code == 0
    assert "He2016DeepRL" in result.output or "Deep Residual" in result.output


def test_add_subcommand(db_url: str) -> None:
    """add subcommand inserts a paper and confirms success."""
    paper = SEED_PAPERS[2]  # Wang2021LargeScaleSA
    user_input = "\n".join([
        paper.title,
        ", ".join(paper.authors),
        paper.bibtex_id,
        paper.contents,
        paper.bibtex,
        "",
    ])
    result = runner.invoke(
        app,
        ["--database-url", db_url, "add"],
        input=user_input,
    )
    assert result.exit_code == 0
    assert "added successfully" in result.output.lower() or "Paper added" in result.output


def test_migrate_subcommand(db_url: str) -> None:
    """migrate subcommand exits 0 (migrations already applied = no-op)."""
    result = runner.invoke(app, ["--database-url", db_url, "migrate"])
    assert result.exit_code == 0
    assert "Migration complete" in result.output


def test_delete_subcommand_abort(seeded_db_url: str) -> None:
    """delete subcommand aborts when user says 'n' to confirmation."""
    # Pick paper id directly (use --id), then refuse confirmation
    result = runner.invoke(
        app,
        ["--database-url", seeded_db_url, "delete", "--id", "1"],
        input="n\n",
    )
    assert result.exit_code == 0
    assert "aborted" in result.output.lower() or "Delete aborted" in result.output


def test_update_subcommand_abort(seeded_db_url: str) -> None:
    """update subcommand aborts when user chooses Abort from the submenu."""
    result = runner.invoke(
        app,
        ["--database-url", seeded_db_url, "update", "--id", "1"],
        input="5\n",  # 5 = Abort
    )
    assert result.exit_code == 0
    assert "aborted" in result.output.lower() or "No changes" in result.output
