"""End-to-end interface-layer tests via Typer's CliRunner.

Every subcommand (and the no-subcommand top-level menu) is exercised through the
public entry point against a migrated/seeded ephemeral database, satisfying the
interface-layer coverage gate.
"""

from __future__ import annotations

from sqlalchemy import Engine
from typer.testing import CliRunner

from paper_sorts.cli.app import app

runner = CliRunner()


def _url(engine: Engine) -> str:
    return str(engine.url)


def test_search_by_title(seeded_engine: Engine) -> None:
    # menu: choose "Search by paper title" (2), enter title
    result = runner.invoke(
        app,
        ["--database-url", _url(seeded_engine), "search"],
        input="2\nLarge-scale Self- an Semi-Supervised learning for speech translation\n",
    )
    assert result.exit_code == 0
    assert "Wang, Changhan" in result.stdout


def test_search_by_author(seeded_engine: Engine) -> None:
    result = runner.invoke(
        app,
        ["--database-url", _url(seeded_engine), "search"],
        input="1\nPino, J.\n1\n",
    )
    assert result.exit_code == 0
    assert "title:" in result.stdout


def test_search_not_found(seeded_engine: Engine) -> None:
    result = runner.invoke(
        app,
        ["--database-url", _url(seeded_engine), "search"],
        input="2\nNo Such Paper\n",
    )
    assert result.exit_code == 0
    assert "not found" in result.stdout.lower()


def test_add_inline(migrated_engine: Engine) -> None:
    result = runner.invoke(
        app,
        ["--database-url", _url(migrated_engine), "add"],
        input="Doe, Jane\nMy New Paper\nDoe2026\n2\n@misc{Doe2026}\nA summary.\n",
    )
    assert result.exit_code == 0
    assert "Added entry" in result.stdout


def test_add_from_file(migrated_engine: Engine, tmp_path) -> None:  # type: ignore[no-untyped-def]
    bib_file = tmp_path / "entry.bib"
    bib_file.write_text("@misc{File2026, title={From File}}", encoding="utf-8")
    result = runner.invoke(
        app,
        ["--database-url", _url(migrated_engine), "add"],
        input=f"Roe, Richard\nFile Paper\nFile2026\n1\n{bib_file}\nSummary.\n",
    )
    assert result.exit_code == 0
    assert "Added entry" in result.stdout


def test_update_title_confirmed(seeded_engine: Engine) -> None:
    from paper_sorts.services.paper_service import PaperService

    pid = PaperService(seeded_engine).search_by_title("Shared Title")[0].paper_id
    result = runner.invoke(
        app,
        ["--database-url", _url(seeded_engine), "update"],
        input=f"1\n1\n{pid}\nNew Name\n1\n",
    )
    assert result.exit_code == 0
    assert PaperService(seeded_engine).search_by_title("New Name")


def test_update_aborted_writes_nothing(seeded_engine: Engine) -> None:
    from paper_sorts.services.paper_service import PaperService

    pid = PaperService(seeded_engine).search_by_title("Shared Title")[0].paper_id
    result = runner.invoke(
        app,
        ["--database-url", _url(seeded_engine), "update"],
        input=f"1\n1\n{pid}\nIgnored\n2\n",  # confirm = No
    )
    assert result.exit_code == 0
    assert PaperService(seeded_engine).search_by_title("Ignored") == []


def test_delete_confirmed(seeded_engine: Engine) -> None:
    from paper_sorts.services.paper_service import PaperService

    result = runner.invoke(
        app,
        ["--database-url", _url(seeded_engine), "delete"],
        input="Direct speech-to-speech translation with discrete units\n1\n",
    )
    assert result.exit_code == 0
    assert (
        PaperService(seeded_engine).search_by_title(
            "Direct speech-to-speech translation with discrete units"
        )
        == []
    )


def test_top_level_menu_quit(seeded_engine: Engine) -> None:
    # no subcommand -> menu; choose abort (option 4)
    result = runner.invoke(app, ["--database-url", _url(seeded_engine)], input="4\n")
    assert result.exit_code == 0
    assert "Closing connection" in result.stdout


def test_top_level_menu_search_then_quit(seeded_engine: Engine) -> None:
    result = runner.invoke(
        app,
        ["--database-url", _url(seeded_engine)],
        input="1\n2\nShared Title\n1\n4\n",
    )
    assert result.exit_code == 0


def test_import_subcommand(migrated_engine: Engine) -> None:
    from pathlib import Path

    fixtures = Path(__file__).parent / "fixtures"
    result = runner.invoke(
        app,
        [
            "--database-url",
            _url(migrated_engine),
            "import",
            "--tex",
            str(fixtures / "literature_overview.tex"),
            "--bib",
            str(fixtures / "sample.bib"),
        ],
    )
    assert result.exit_code == 0
    assert "Imported 2" in result.stdout


def test_migrate_subcommand(ephemeral_db_url: str) -> None:
    result = runner.invoke(app, ["--database-url", ephemeral_db_url, "migrate"])
    assert result.exit_code == 0


def test_missing_database_url_errors() -> None:
    import os

    env = {k: v for k, v in os.environ.items() if not k.startswith("PDBSEARCH_")}
    result = runner.invoke(app, ["search"], env=env)
    assert result.exit_code != 0
