"""End-to-end interface-layer tests via Typer's ``CliRunner``.

Exercises every subcommand and the bare-invocation menu through the public
entry point, scripting interactive prompts by patching ``Prompt.ask``. This
satisfies the interface-layer coverage gate (constitution Principle II G1).
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from sqlalchemy import Engine, text
from typer.testing import CliRunner

from paper_sorts.cli import prompts
from paper_sorts.cli.app import app
from paper_sorts.db.session import with_session

runner = CliRunner()


@pytest.fixture
def script(monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
    """Return a helper that scripts successive ``Prompt.ask`` answers.

    :param monkeypatch: pytest's monkeypatch fixture.
    :returns: a callable taking the list of scripted answers.
    """

    def _install(answers: list[str]) -> None:
        it: Iterator[str] = iter(answers)
        monkeypatch.setattr(prompts.Prompt, "ask", staticmethod(lambda *a, **k: next(it)))

    return _install


def _invoke(engine: Engine, args: list[str]) -> object:
    """Invoke the CLI with ``--database-url`` pointed at the test engine.

    :param engine: the ephemeral engine.
    :param args: the subcommand and its arguments.
    :returns: the Click ``Result``.
    """
    return runner.invoke(app, ["--database-url", str(engine.url), *args])


def _seed_one(engine: Engine, script) -> None:  # type: ignore[no-untyped-def]
    """Add a single known paper through the ``add`` subcommand.

    :param engine: the ephemeral engine.
    :param script: the prompt-scripting helper.
    """
    script(
        [
            "Doe, J.",  # authors (single "Last, First", ';'-separated list)
            "CLI Paper",  # title
            "clikey",  # bibtex key
            "2",  # bib via file? -> No
            "@misc{clikey}",  # inline bib
            "A CLI summary.",  # summary
        ]
    )
    result = _invoke(engine, ["add"])
    assert result.exit_code == 0, result.output


def test_add_then_search_title(engine: Engine, script) -> None:  # type: ignore[no-untyped-def]
    """An added paper is found by a subsequent title search."""
    _seed_one(engine, script)
    script(["2", "CLI Paper"])  # method=title, title
    result = _invoke(engine, ["search"])
    assert result.exit_code == 0
    assert "A CLI summary." in result.output
    assert "Doe, J." in result.output


def test_search_by_author(seeded_engine: Engine, script) -> None:  # type: ignore[no-untyped-def]
    """The seeded ``Pino, J.`` is found via the author search path."""
    # Multiple matches → disambiguation picks the first.
    script(["1", "Pino, J.", "1"])
    result = _invoke(seeded_engine, ["search"])
    assert result.exit_code == 0
    assert "bib entry:" in result.output


def test_search_missing_title(engine: Engine, script) -> None:  # type: ignore[no-untyped-def]
    """A missing title yields a plain not-found message."""
    script(["2", "nope"])
    result = _invoke(engine, ["search"])
    assert "not found" in result.output.lower()


def test_update_title_with_confirmation(engine: Engine, script) -> None:  # type: ignore[no-untyped-def]
    """Updating a title with confirmation persists the change."""
    _seed_one(engine, script)
    with with_session(engine) as session:
        pid = session.execute(text("SELECT id FROM papers WHERE bibtex_id='clikey'")).scalar_one()
    script(["1", "1", str(pid), "Renamed CLI Paper", "y"])
    result = _invoke(engine, ["update"])
    assert result.exit_code == 0
    assert "Update applied." in result.output
    script(["2", "Renamed CLI Paper"])
    found = _invoke(engine, ["search"])
    assert "Renamed CLI Paper" in found.output


def test_update_abort_writes_nothing(engine: Engine, script) -> None:  # type: ignore[no-untyped-def]
    """Declining the confirmation leaves the title unchanged."""
    _seed_one(engine, script)
    with with_session(engine) as session:
        pid = session.execute(text("SELECT id FROM papers WHERE bibtex_id='clikey'")).scalar_one()
    script(["1", "1", str(pid), "Should Not Stick", "n"])
    result = _invoke(engine, ["update"])
    assert "Stopping update process" in result.output
    script(["2", "Should Not Stick"])
    found = _invoke(engine, ["search"])
    assert "not found" in found.output.lower()


def test_delete_with_confirmation(engine: Engine, script) -> None:  # type: ignore[no-untyped-def]
    """Deleting a paper with confirmation removes it."""
    _seed_one(engine, script)
    script(["clikey", "CLI Paper", "1"])  # key, title, confirm
    result = _invoke(engine, ["delete"])
    assert result.exit_code == 0
    assert "Deleted" in result.output
    script(["2", "CLI Paper"])
    found = _invoke(engine, ["search"])
    assert "not found" in found.output.lower()


def test_interactive_menu_quit(engine: Engine, script) -> None:  # type: ignore[no-untyped-def]
    """The bare menu runs and exits cleanly on quit."""
    script(["q"])
    result = _invoke(engine, [])
    assert result.exit_code == 0
    assert "Closing connection" in result.output


def test_missing_database_url_errors() -> None:
    """No configured database URL yields a clear error and non-zero exit."""
    result = runner.invoke(app, ["search"])
    assert result.exit_code == 1
    assert "Configuration error" in result.output


def test_add_via_bib_file(engine: Engine, script, tmp_path) -> None:  # type: ignore[no-untyped-def]
    """The add flow can read the bibtex entry from a file."""
    bibfile = tmp_path / "entry.bib"
    bibfile.write_text("@misc{filekey}", encoding="utf-8")
    script(
        [
            "Roe, R.",
            "File Paper",
            "filekey",
            "1",  # bib via file? -> Yes
            str(bibfile),
            "From a file.",
        ]
    )
    result = _invoke(engine, ["add"])
    assert result.exit_code == 0
    assert "Added 'File Paper'." in result.output


def test_add_missing_bib_file(engine: Engine, script) -> None:  # type: ignore[no-untyped-def]
    """A missing bib file surfaces a plain message, not a traceback."""
    script(["Roe, R.", "Bad Paper", "badkey", "1", "/no/such/file.bib"])
    result = _invoke(engine, ["add"])
    assert "Could not read the bib file" in result.output


def test_add_duplicate_key_message(engine: Engine, script) -> None:  # type: ignore[no-untyped-def]
    """Adding a duplicate bibtex key yields a plain rejection message."""
    _seed_one(engine, script)
    script(["Doe, J.", "Another", "clikey", "2", "@misc{clikey}", "dup"])
    result = _invoke(engine, ["add"])
    assert "already exists" in result.output


def test_add_abort_on_bib_choice(engine: Engine, script) -> None:  # type: ignore[no-untyped-def]
    """Aborting at the bib-source choice cancels the add."""
    script(["Doe, J.", "Abort Paper", "abkey", "3"])  # 3 -> abort
    result = _invoke(engine, ["add"])
    assert "Add aborted." in result.output


def test_update_bib_path(engine: Engine, script) -> None:  # type: ignore[no-untyped-def]
    """The bib update path replaces the bibtex string."""
    _seed_one(engine, script)
    script(["2", "clikey", "@misc{clikey, note={new}}", "y"])  # table=bib
    result = _invoke(engine, ["update"])
    assert "Update applied." in result.output


def test_update_author_path(engine: Engine, script) -> None:  # type: ignore[no-untyped-def]
    """The authors update path renames the author."""
    _seed_one(engine, script)
    script(["3", "Doe, J.", "Doe, Jane", "y"])  # table=authors
    result = _invoke(engine, ["update"])
    assert "Update applied." in result.output
    script(["1", "Doe, Jane", "1"])
    found = _invoke(engine, ["search"])
    assert found.exit_code == 0


def test_update_abort_at_table_menu(engine: Engine, script) -> None:  # type: ignore[no-untyped-def]
    """Aborting at the update table menu stops the process."""
    script(["4"])  # abort
    result = _invoke(engine, ["update"])
    assert "Stopping update process" in result.output


def test_delete_missing_message(engine: Engine, script) -> None:  # type: ignore[no-untyped-def]
    """Deleting a non-existent paper yields a plain message."""
    script(["ghostkey", "Ghost", "1"])
    result = _invoke(engine, ["delete"])
    assert "no such paper" in result.output.lower()


def test_delete_abort(engine: Engine, script) -> None:  # type: ignore[no-untyped-def]
    """Declining the delete confirmation cancels it."""
    _seed_one(engine, script)
    script(["clikey", "CLI Paper", "n"])
    result = _invoke(engine, ["delete"])
    assert "Stopping delete process" in result.output


def test_menu_routes_search_add_update(engine: Engine, script) -> None:  # type: ignore[no-untyped-def]
    """The bare menu routes 1/2/3 to search/add/update then quits."""
    script(
        [
            "2",  # menu: Add
            "Menu, M.",
            "Menu Paper",
            "menukey",
            "2",
            "@misc{menukey}",
            "via menu",
            "1",  # menu: Search
            "2",
            "Menu Paper",  # search by title
            "4",  # menu: Quit
        ]
    )
    result = _invoke(engine, [])
    assert result.exit_code == 0
    assert "via menu" in result.output
    assert "Closing connection" in result.output


def test_menu_invalid_then_quit(engine: Engine, script) -> None:  # type: ignore[no-untyped-def]
    """An invalid menu choice is reported, then the menu quits."""
    script(["x", "4"])
    result = _invoke(engine, [])
    assert "Your input was invalid" in result.output
