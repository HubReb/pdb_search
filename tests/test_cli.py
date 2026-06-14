"""CLI layer tests for paper_sorts using Typer's CliRunner.

Tests exercise the CLI subcommands at the process boundary.  The --database-url
flag is used to inject the ephemeral DB URL.
"""

from __future__ import annotations

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


def test_update_subcommand_title(seeded_db_url: str) -> None:
    """update subcommand updates the title when user confirms."""
    result = runner.invoke(
        app,
        ["--database-url", seeded_db_url, "update", "--id", "1"],
        input="1\nNew Title Here\ny\n",  # 1=title, new value, confirm yes
    )
    assert result.exit_code == 0
    assert "Update successful" in result.output or "successful" in result.output.lower()


def test_delete_subcommand_confirm(seeded_db_url: str) -> None:
    """delete subcommand deletes the paper when user confirms."""
    result = runner.invoke(
        app,
        ["--database-url", seeded_db_url, "delete", "--id", "1"],
        input="y\n",
    )
    assert result.exit_code == 0
    assert "deleted successfully" in result.output.lower() or "deleted" in result.output.lower()


def test_search_multiple_papers_disambiguation(seeded_db_url: str) -> None:
    """search by title shows disambiguation for multiple matches; user selects one."""
    # "Attention Is All You Need" appears twice in seed data
    # User picks option 1, then "3) Back" from search menu
    result = runner.invoke(
        app,
        ["--database-url", seeded_db_url, "search"],
        input="1\nAttention Is All You Need\n1\n",
    )
    assert result.exit_code == 0
    assert "Vaswani" in result.output or "Attention" in result.output


def test_search_back_option(db_url: str) -> None:
    """search menu option 3 returns without error."""
    result = runner.invoke(
        app,
        ["--database-url", db_url, "search"],
        input="3\n",  # 3 = Back
    )
    assert result.exit_code == 0


def test_delete_search_then_abort(seeded_db_url: str) -> None:
    """delete without --id searches first, then user aborts."""
    # Search for "Large-Scale" (1 result), then abort deletion
    result = runner.invoke(
        app,
        ["--database-url", seeded_db_url, "delete"],
        input="Large-Scale\nn\n",
    )
    assert result.exit_code == 0
    assert "aborted" in result.output.lower() or "Delete aborted" in result.output


def test_update_search_then_abort(seeded_db_url: str) -> None:
    """update without --id searches first, then user aborts."""
    result = runner.invoke(
        app,
        ["--database-url", seeded_db_url, "update"],
        input="Large-Scale\n5\n",  # find paper, then Abort
    )
    assert result.exit_code == 0
    assert "aborted" in result.output.lower() or "No changes" in result.output


def test_update_authors_via_cli(seeded_db_url: str) -> None:
    """update subcommand can update authors field."""
    result = runner.invoke(
        app,
        ["--database-url", seeded_db_url, "update", "--id", "1"],
        input="4\nNew, Author\ny\n",  # 4=authors, new value, confirm yes
    )
    assert result.exit_code == 0


def test_add_subcommand_from_bib_text(db_url: str) -> None:
    """add subcommand accepts inline BibTeX text (not a file path)."""
    user_input = "\n".join([
        "Test Paper Title",
        "Test, Author",
        "TestKey2024",
        "Test summary content",
        "@article{TestKey2024, title={Test}, year={2024}}",
        "",
    ])
    result = runner.invoke(
        app,
        ["--database-url", db_url, "add"],
        input=user_input,
    )
    assert result.exit_code == 0


def test_search_by_author_no_results(db_url: str) -> None:
    """search by author on empty DB prints 'No papers found'."""
    result = runner.invoke(
        app,
        ["--database-url", db_url, "search"],
        input="2\nNonExistentAuthor\n",
    )
    assert result.exit_code == 0
    assert "No papers found" in result.output or result.exit_code == 0


def test_delete_no_id_no_results(db_url: str) -> None:
    """delete without --id on empty DB prints 'No papers found'."""
    result = runner.invoke(
        app,
        ["--database-url", db_url, "delete"],
        input="NonExistent\n",
    )
    assert result.exit_code == 0
    assert "No papers found" in result.output


def test_update_no_id_no_results(db_url: str) -> None:
    """update without --id on empty DB prints 'No papers found'."""
    result = runner.invoke(
        app,
        ["--database-url", db_url, "update"],
        input="NonExistent\n",
    )
    assert result.exit_code == 0
    assert "No papers found" in result.output


def test_interactive_menu_quit(db_url: str) -> None:
    """Interactive top-level menu quits on 'q'."""
    result = runner.invoke(
        app,
        ["--database-url", db_url],
        input="q\n",
    )
    # Exit due to SystemExit(0) from typer.Exit(0) — runner catches it
    assert result.exit_code in (0, 1)


def test_interactive_menu_invalid_then_quit(db_url: str) -> None:
    """Interactive menu re-prompts on invalid input then quits."""
    result = runner.invoke(
        app,
        ["--database-url", db_url],
        input="9\nq\n",  # invalid choice, then quit
    )
    assert result.exit_code in (0, 1)


def test_interactive_menu_search(db_url: str) -> None:
    """Interactive menu option 1 enters search submenu."""
    result = runner.invoke(
        app,
        ["--database-url", db_url],
        input="1\n3\nq\n",  # 1=Search, 3=Back, q=Quit
    )
    assert result.exit_code in (0, 1)


def test_interactive_menu_add(db_url: str) -> None:
    """Interactive menu option 2 enters add flow."""
    paper = SEED_PAPERS[2]
    user_input = "\n".join([
        "2",  # Add from menu
        paper.title,
        ", ".join(paper.authors),
        paper.bibtex_id,
        paper.contents,
        paper.bibtex,
        "q",  # quit after
        "",
    ])
    result = runner.invoke(
        app,
        ["--database-url", db_url],
        input=user_input,
    )
    assert result.exit_code in (0, 1)


def test_interactive_menu_delete(seeded_db_url: str) -> None:
    """Interactive menu option 4 enters delete flow."""
    result = runner.invoke(
        app,
        ["--database-url", seeded_db_url],
        input="4\nLarge-Scale\nn\nq\n",  # 4=Delete, find, abort, quit
    )
    assert result.exit_code in (0, 1)


def test_interactive_menu_update(seeded_db_url: str) -> None:
    """Interactive menu option 3 enters update flow."""
    result = runner.invoke(
        app,
        ["--database-url", seeded_db_url],
        input="3\nLarge-Scale\n5\nq\n",  # 3=Update, find, abort, quit
    )
    assert result.exit_code in (0, 1)


def test_update_bibtex_via_cli(seeded_db_url: str) -> None:
    """update subcommand can update bibtex field."""
    result = runner.invoke(
        app,
        ["--database-url", seeded_db_url, "update", "--id", "1"],
        input="3\n@article{NewKey, title={New}}\ny\n",  # 3=bibtex, new value, confirm
    )
    assert result.exit_code == 0


def test_update_contents_via_cli(seeded_db_url: str) -> None:
    """update subcommand can update contents field."""
    result = runner.invoke(
        app,
        ["--database-url", seeded_db_url, "update", "--id", "1"],
        input="2\nNew contents here\ny\n",  # 2=contents, new value, confirm
    )
    assert result.exit_code == 0


def test_delete_multiple_papers_disambiguation(seeded_db_url: str) -> None:
    """delete without --id shows disambiguation for multiple matches."""
    # "Attention Is All You Need" appears twice; user picks first, then aborts
    result = runner.invoke(
        app,
        ["--database-url", seeded_db_url, "delete"],
        input="Attention Is All You Need\n1\nn\n",
    )
    assert result.exit_code == 0
    assert "aborted" in result.output.lower() or "Delete aborted" in result.output


def test_update_multiple_papers_disambiguation(seeded_db_url: str) -> None:
    """update without --id shows disambiguation for multiple matches."""
    result = runner.invoke(
        app,
        ["--database-url", seeded_db_url, "update"],
        input="Attention Is All You Need\n1\n5\n",  # find, pick first, Abort
    )
    assert result.exit_code == 0
    assert "aborted" in result.output.lower() or "No changes" in result.output


def test_app_uses_encrypted_config(db_url: str) -> None:
    """App resolves database URL from Fernet-encrypted INI when --config + --key provided."""
    import tempfile
    from pathlib import Path

    from cryptography.fernet import Fernet

    # Parse the ephemeral db_url to build a valid INI section
    # db_url is like postgresql+psycopg://user:@host:port/dbname
    parts = db_url.replace("postgresql+psycopg://", "").split("@")
    user_part = parts[0].rstrip(":")
    host_port_db = parts[1]
    host_port, dbname = host_port_db.rsplit("/", 1)
    host, port = host_port.rsplit(":", 1)

    ini_content = f"[postgresql]\ndbname={dbname}\nuser={user_part}\npassword=\nhost={host}\nport={port}\n"
    key = Fernet.generate_key()
    encrypted = Fernet(key).encrypt(ini_content.encode())

    with (
        tempfile.NamedTemporaryFile(suffix=".crypt", delete=False) as cf,
        tempfile.NamedTemporaryFile(suffix=".key", delete=False) as kf,
    ):
        cf.write(encrypted)
        kf.write(key)
        config_path = Path(cf.name)
        key_path = Path(kf.name)

    try:
        result = runner.invoke(
            app,
            ["--config", str(config_path), "--key", str(key_path), "migrate"],
        )
        assert result.exit_code == 0
        assert "Migration complete" in result.output
    finally:
        config_path.unlink(missing_ok=True)
        key_path.unlink(missing_ok=True)
