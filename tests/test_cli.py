"""End-to-end CLI tests via Typer's CliRunner over a seeded ephemeral DB.

Each invocation passes the ephemeral database URL with ``--database-url`` and
feeds the interactive prompts through stdin. These cover the behaviour-parity
paths from US2 (SC-002): search one/multiple matches, search by author, add
inline, add from a file, update with confirm yes/no, delete, quit, empty-input
re-prompt, and a plain-language error on failure.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import Engine
from typer.testing import CliRunner

from paper_sorts.cli.app import app
from paper_sorts.db.session import with_session
from tests.conftest import _seed

runner = CliRunner()


@pytest.fixture
def db_url(engine: Engine, ephemeral_db_url: str) -> str:
    """Seed the ephemeral DB and return its URL for --database-url."""
    with with_session(engine) as session:
        _seed(session)
    return ephemeral_db_url


def _run(db_url: str, args: list[str], stdin: str) -> str:
    """Invoke the CLI with a database URL and piped stdin; return combined output."""
    result = runner.invoke(app, ["--database-url", db_url, *args], input=stdin)
    assert result.exit_code == 0, result.output
    return result.output


def test_search_by_title_single(db_url: str) -> None:
    """Searching a unique title prints its authors and bib."""
    out = _run(
        db_url,
        ["search"],
        "2\nDirect speech-to-speech translation with discrete units\n",
    )
    assert "Lee, Ann and Chen, Peng-Jen and Pino, J." in out
    assert "Lee2022Direct" in out


def test_search_by_title_multiple_disambiguates(db_url: str) -> None:
    """A shared title prompts a 1-indexed choice, then prints the pick."""
    out = _run(db_url, ["search"], "2\nOn Calibration\n2\n")
    assert "Smith, Jane" in out


def test_search_by_author(db_url: str) -> None:
    """Searching by author with multiple papers disambiguates."""
    out = _run(db_url, ["search"], "1\nPino, J.\n1\n")
    assert "title:" in out


def test_search_not_found(db_url: str) -> None:
    """A missing title yields a plain-language not-found message."""
    out = _run(db_url, ["search"], "2\nnonexistent title\n")
    assert "not found" in out.lower()


def test_add_inline_then_searchable(db_url: str) -> None:
    """Adding inline persists a paper retrievable by title.

    Authors are a comma-separated ``Last, First`` list split on ``", "`` — the
    preserved legacy semantics, so a two-author entry uses two list items.
    """
    add_in = "Lovelace, Ada\nThe Engine\nEngine1843\n2\n@book{Engine1843}\nClassic.\n"
    out = _run(db_url, ["add"], add_in)
    assert "Added entry" in out
    found = _run(db_url, ["search"], "2\nThe Engine\n")
    # The author line is split on ", " (preserved legacy semantics), so a single
    # "Last, First" name becomes two list items joined with " and " on display.
    assert "Lovelace and Ada" in found


def test_add_from_file(db_url: str, tmp_path: Path) -> None:
    """Adding with the BibTeX entry read from a file persists the paper."""
    bib = tmp_path / "entry.bib"
    bib.write_text("@article{FromFile2026, title={From File}}", encoding="utf-8")
    add_in = f"File, Author\nFrom File Paper\nFromFile2026\n1\n{bib}\nSummary here.\n"
    out = _run(db_url, ["add"], add_in)
    assert "Added entry" in out
    assert _run(db_url, ["search"], "2\nFrom File Paper\n").count("From File") >= 1


def test_add_empty_input_reprompts(db_url: str) -> None:
    """An empty author line re-prompts before accepting input."""
    # First author line is blank (re-prompt), then a real value.
    add_in = "\nReal, Author\nReprompt Title\nReprompt1\n2\n@misc{Reprompt1}\nSum.\n"
    out = _run(db_url, ["add"], add_in)
    assert "Added entry" in out


def test_update_title_confirm_yes(db_url: str) -> None:
    """Updating a title with 'y' persists the new title."""
    add_in = "U, Author\nOriginal Title\nUpd1\n2\n@misc{Upd1}\nSum.\n"
    _run(db_url, ["add"], add_in)
    # find the paper id by searching is indirect; update by papers/title needs id.
    # Use the service to get the id deterministically.
    from sqlalchemy import create_engine, select

    from paper_sorts.db.models import Paper

    eng = create_engine(db_url)
    with with_session(eng) as session:
        paper_id = session.scalar(select(Paper.id).where(Paper.bibtex_id == "Upd1"))
    out = _run(db_url, ["update"], f"1\n1\n{paper_id}\nNew Title\ny\n")
    assert "updated" in out.lower()
    assert "New Title" in _run(db_url, ["search"], "2\nNew Title\n")


def test_update_confirm_no_writes_nothing(db_url: str) -> None:
    """Updating then answering 'n' leaves the title unchanged."""
    add_in = "V, Author\nKeep Title\nKeep1\n2\n@misc{Keep1}\nSum.\n"
    _run(db_url, ["add"], add_in)
    from sqlalchemy import create_engine, select

    from paper_sorts.db.models import Paper

    eng = create_engine(db_url)
    with with_session(eng) as session:
        paper_id = session.scalar(select(Paper.id).where(Paper.bibtex_id == "Keep1"))
    _run(db_url, ["update"], f"1\n1\n{paper_id}\nShould Not Apply\nn\n")
    assert _run(db_url, ["search"], "2\nKeep Title\n").count("Keep Title") >= 1
    assert "Should Not Apply" not in _run(db_url, ["search"], "2\nShould Not Apply\n")


def test_delete_confirm(db_url: str) -> None:
    """Deleting with confirmation removes the paper."""
    out = _run(
        db_url,
        ["delete"],
        "Direct speech-to-speech translation with discrete units\n1\ny\n",
    )
    assert "deleted" in out.lower()
    gone = _run(
        db_url,
        ["search"],
        "2\nDirect speech-to-speech translation with discrete units\n",
    )
    assert "not found" in gone.lower()


def test_top_level_menu_quit(db_url: str) -> None:
    """The no-subcommand menu quits cleanly on the abort option."""
    result = runner.invoke(app, ["--database-url", db_url], input="4\n")
    assert result.exit_code == 0
    assert "Closing connection" in result.output


def test_missing_database_url_is_plain_error() -> None:
    """Running a subcommand with no configured DB shows a plain message."""
    result = runner.invoke(app, ["search"], input="2\nx\n")
    assert result.exit_code == 1
    assert "No database configured" in result.output
