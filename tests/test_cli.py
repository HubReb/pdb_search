"""Interface-layer tests: drive every subcommand through Typer's CliRunner.

These run against the real ephemeral database (passed via ``--database-url``) and
feed scripted stdin to the interactive prompts, mirroring the legacy end-to-end
dialog script. Covers search/add/update/delete plus the no-subcommand menu and
the abort/quit/empty-reprompt/confirm-no paths.
"""

from __future__ import annotations

from typer.testing import CliRunner

from paper_sorts.cli.app import app

runner = CliRunner()


def _g(url: str, *args: str) -> list[str]:
    """Build a CLI argv with the global ``--database-url`` option.

    :param url: the database URL.
    :param args: the subcommand and its arguments.
    :return: the full argv list.
    """
    return ["--database-url", url, *args]


def test_search_by_title_single(seeded_db_url: str) -> None:
    """search → by title → unique match prints the pretty-print block."""
    result = runner.invoke(
        app,
        _g(seeded_db_url, "search"),
        input="2\nDirect speech-to-speech translation with discrete units\n",
    )
    assert result.exit_code == 0
    assert "Lee, Ann and Chen, Peng-Jen" in result.output
    assert "bib entry:" in result.output


def test_search_by_title_disambiguation(seeded_db_url: str) -> None:
    """search → by title → shared title prompts a 1-indexed disambiguation."""
    result = runner.invoke(
        app,
        _g(seeded_db_url, "search"),
        input="2\nAttention is all you need\n1\n",
    )
    assert result.exit_code == 0
    assert "Multiple papers match" in result.output


def test_search_by_author(seeded_db_url: str) -> None:
    """search → by author lists the author's papers."""
    result = runner.invoke(app, _g(seeded_db_url, "search"), input="1\nPino, J.\n")
    assert result.exit_code == 0
    assert "Direct speech-to-speech translation with discrete units" in result.output


def test_search_not_found(seeded_db_url: str) -> None:
    """A missing title yields a plain message, not a traceback."""
    result = runner.invoke(app, _g(seeded_db_url, "search"), input="2\nNo such paper\n")
    assert result.exit_code == 0
    assert "No papers found" in result.output


def test_search_abort(seeded_db_url: str) -> None:
    """Choosing the abort option exits the search flow cleanly."""
    result = runner.invoke(app, _g(seeded_db_url, "search"), input="3\n")
    assert result.exit_code == 0


def test_add_inline(seeded_db_url: str) -> None:
    """add with an inline bib persists and is retrievable by title."""
    add = runner.invoke(
        app,
        _g(seeded_db_url, "add"),
        input=(
            "New, Author\n"  # authors
            "Newly added paper\n"  # title
            "New2026Key\n"  # bibtex key
            "2\n"  # inline bib (option 2 = No file)
            "@article{New2026Key, title={Newly added paper}}\n"  # bib
            "A summary.\n"  # summary
        ),
    )
    assert add.exit_code == 0, add.output
    assert "Added" in add.output
    found = runner.invoke(app, _g(seeded_db_url, "search"), input="2\nNewly added paper\n")
    assert "New2026Key" in found.output


def test_add_empty_reprompt(seeded_db_url: str) -> None:
    """An empty title is re-prompted until non-empty input is given."""
    add = runner.invoke(
        app,
        _g(seeded_db_url, "add"),
        input=(
            "Re, Prompt\n"
            "\n"  # empty title -> re-prompt
            "Reprompted title\n"
            "Re2026Key\n"
            "2\n"
            "@article{Re2026Key}\n"
            "summary\n"
        ),
    )
    assert add.exit_code == 0, add.output
    assert "Added" in add.output


def test_add_duplicate_is_plain(seeded_db_url: str) -> None:
    """Adding a duplicate BibTeX key shows a plain message, not a traceback."""
    add = runner.invoke(
        app,
        _g(seeded_db_url, "add"),
        input=(
            "Dup, Author\n"
            "Dup title\n"
            "Wang2021LargeScaleSA\n"  # existing key
            "2\n"
            "@article{Wang2021LargeScaleSA}\n"
            "summary\n"
        ),
    )
    assert add.exit_code == 0
    assert "already exists" in add.output


def test_update_title_confirm_yes(seeded_db_url: str) -> None:
    """update papers.title with confirm=yes persists the change."""
    find = runner.invoke(
        app,
        _g(seeded_db_url, "search"),
        input="2\nDirect speech-to-speech translation with discrete units\n",
    )
    assert find.exit_code == 0
    upd = runner.invoke(
        app,
        _g(seeded_db_url, "update"),
        input="1\n1\n2\nRenamed by CLI\ny\n",  # papers, title, id=2, value, yes
    )
    assert upd.exit_code == 0, upd.output
    assert "Update applied" in upd.output


def test_update_confirm_no_writes_nothing(seeded_db_url: str) -> None:
    """Declining the confirmation writes nothing."""
    upd = runner.invoke(
        app,
        _g(seeded_db_url, "update"),
        input="1\n1\n1\nShould not stick\nn\n",
    )
    assert upd.exit_code == 0
    assert "No change made" in upd.output


def test_update_abort(seeded_db_url: str) -> None:
    """Aborting at the table menu makes no change."""
    upd = runner.invoke(app, _g(seeded_db_url, "update"), input="4\n")
    assert upd.exit_code == 0


def test_delete_confirm_yes(seeded_db_url: str) -> None:
    """delete with confirm=yes removes the paper and its bib row."""
    result = runner.invoke(
        app,
        _g(seeded_db_url, "delete"),
        input="Direct speech-to-speech translation with discrete units\ny\n",
    )
    assert result.exit_code == 0, result.output
    assert "Deleted" in result.output


def test_delete_confirm_no(seeded_db_url: str) -> None:
    """Declining the delete confirmation keeps the paper."""
    result = runner.invoke(
        app,
        _g(seeded_db_url, "delete"),
        input="Direct speech-to-speech translation with discrete units\nn\n",
    )
    assert result.exit_code == 0
    assert "Nothing deleted" in result.output


def test_menu_quit(seeded_db_url: str) -> None:
    """The no-subcommand menu quits on the abort option."""
    result = runner.invoke(app, _g(seeded_db_url), input="4\n")
    assert result.exit_code == 0
    assert "What do you want to do?" in result.output


def test_menu_search_then_quit(seeded_db_url: str) -> None:
    """The menu can run a search, then quit."""
    result = runner.invoke(
        app,
        _g(seeded_db_url),
        input="1\n1\nPino, J.\n4\n",  # search, by author, name, then quit
    )
    assert result.exit_code == 0
    assert "Direct speech-to-speech translation with discrete units" in result.output


def test_missing_database_url_is_plain() -> None:
    """A missing database URL yields a plain message and non-zero exit."""
    result = runner.invoke(app, ["search"], input="3\n")
    assert result.exit_code != 0
    assert "No database URL configured" in result.output


def test_migrate_smoke(seeded_db_url: str) -> None:
    """migrate on an already-canonical seeded DB is a no-op success."""
    result = runner.invoke(app, _g(seeded_db_url, "migrate"))
    assert result.exit_code == 0
    assert "Migration complete" in result.output


def test_import_smoke(seeded_db_url: str, tmp_path: object) -> None:
    """import reads a .tex + .bib pair and reports the count."""
    import pathlib

    base = pathlib.Path(str(tmp_path))
    tex = base / "lit.tex"
    bib = base / "refs.bib"
    tex.write_text(r"See \cite{Imp2026Key}.", encoding="utf-8")
    bib.write_text(
        "@article{Imp2026Key, title={Imported paper}, author={Imp, Author}, year={2026}}",
        encoding="utf-8",
    )
    result = runner.invoke(app, _g(seeded_db_url, "import", "--tex", str(tex), "--bib", str(bib)))
    assert result.exit_code == 0, result.output
    assert "Imported 1 paper" in result.output
