"""Typer application entry point for paper_sorts.

Wires all subcommands (search, add, update, delete, import, migrate) and drops
into an interactive four-option menu when invoked with no subcommand.

`migrate` and `import` are subcommand-only — not in the four-option menu.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Annotated

import typer
from sqlalchemy import Engine

from paper_sorts.cli import prompts
from paper_sorts.cli.add import run_add
from paper_sorts.cli.delete import run_delete
from paper_sorts.cli.search import run_search
from paper_sorts.cli.update import run_update
from paper_sorts.logging_config import setup_logging

logger = logging.getLogger("paper_sorts.cli.app")

app = typer.Typer(
    name="pdbsearch",
    help="Off-line paper-database searcher: search, add, update, delete, and bulk-import papers.",
    no_args_is_help=False,
    invoke_without_command=True,
)


def _run_interactive_menu(engine: Engine) -> None:
    """Run the four-option interactive top-level menu.

    Menu options:
    1) Search the database
    2) Add an entry
    3) Update an entry
    4) (Q)uit

    'migrate' and 'import' are absent from this menu (admin/scripted operations).

    :param engine: active SQLAlchemy engine
    :type engine: Engine
    """
    while True:
        choice = prompts.ask_choice(
            [
                "Search the database",
                "Add an entry",
                "Update an entry",
            ],
            header="What do you want to do?",
            abort_label="Quit",
        )

        if choice is prompts.ABORT:
            print("Closing connection...")
            break
        elif choice == 0:
            run_search(engine)
        elif choice == 1:
            run_add(engine)
        elif choice == 2:
            run_update(engine)


@app.callback()
def main(
    ctx: typer.Context,
    database_url: Annotated[
        str | None,
        typer.Option("--database-url", help="PostgreSQL DSN (overrides all other config)"),
    ] = None,
    log_level: Annotated[
        str,
        typer.Option("--log-level", help="Logging level"),
    ] = "INFO",
    config: Annotated[
        str | None,
        typer.Option("--config", "-c", help="Fernet-encrypted INI config file"),
    ] = None,
    key: Annotated[
        str | None,
        typer.Option("--key", "-k", help="Key file for decrypting --config"),
    ] = None,
) -> None:
    """Paper sorts: off-line paper-database searcher.

    Run without a subcommand for an interactive menu.
    Run with a subcommand (search, add, update, delete, import, migrate) for direct access.
    """
    # Configure logging before anything else
    setup_logging(log_level=log_level)

    # Build settings and engine
    from paper_sorts.config import Settings
    from paper_sorts.db.session import get_engine

    try:
        init_kwargs: dict[str, object] = {}
        if database_url:
            init_kwargs["database_url"] = database_url
        if config:
            init_kwargs["config_file"] = Path(config)
        if key:
            init_kwargs["key_file"] = Path(key)

        settings = Settings(**init_kwargs)  # type: ignore[arg-type]
        url = settings.get_database_url()
    except (ValueError, FileNotFoundError) as exc:
        print(f"Configuration error: {exc}")
        logger.error("Configuration error: %s", exc)
        raise typer.Exit(code=1) from exc

    engine = get_engine(url)
    ctx.ensure_object(dict)
    ctx.obj = engine

    # If no subcommand was given, run interactive menu
    if ctx.invoked_subcommand is None:
        _run_interactive_menu(engine)


@app.command("search")
def search_command(ctx: typer.Context) -> None:
    """Search the database for papers by author or title."""
    engine: Engine = ctx.obj
    try:
        run_search(engine)
    except Exception as exc:  # noqa: BLE001
        logger.error("Search failed: %s", exc)
        print("Search failed. Check logs for details.")
        sys.exit(1)


@app.command("add")
def add_command(ctx: typer.Context) -> None:
    """Add a new paper entry to the database."""
    engine: Engine = ctx.obj
    success = run_add(engine)
    if not success:
        sys.exit(1)


@app.command("update")
def update_command(ctx: typer.Context) -> None:
    """Update a field of an existing paper entry."""
    engine: Engine = ctx.obj
    success = run_update(engine)
    if not success:
        sys.exit(1)


@app.command("delete")
def delete_command(ctx: typer.Context) -> None:
    """Delete a paper entry from the database."""
    engine: Engine = ctx.obj
    success = run_delete(engine)
    if not success:
        sys.exit(1)


@app.command("import")
def import_command(
    ctx: typer.Context,
    tex: Path = typer.Option(..., "--tex", help="Path to the .tex file", exists=True),
    bib: Path = typer.Option(..., "--bib", help="Path to the .bib file", exists=True),
) -> None:
    """Bulk-import papers from a LaTeX + BibTeX file pair (per-paper commit)."""
    from paper_sorts.services.import_service import extract_papers_from_tex_bib
    from paper_sorts.services import paper_service

    engine: Engine = ctx.obj
    inserted = 0
    skipped = 0
    failed = 0

    try:
        papers = list(extract_papers_from_tex_bib(tex, bib))
    except FileNotFoundError as exc:
        print(f"File not found: {exc}")
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001
        print(f"Failed to parse input files: {exc}")
        logger.error("import parsing failed: %s", exc)
        sys.exit(1)

    print(f"Found {len(papers)} paper(s) to import.")

    from paper_sorts.db.session import with_session

    for p in papers:
        try:
            with with_session(engine) as session:
                paper_service.add_paper(session, p)
            inserted += 1
            print(f"  Imported: {p.title} (key: {p.bibtex_id})")
        except ValueError as exc:
            skipped += 1
            logger.warning("Skipping '%s': %s", p.title, exc)
            print(f"  Skipped: {p.title} — {exc}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            logger.error("Failed to import '%s': %s", p.title, exc)
            print(f"  Failed: {p.title} — check logs")

    print(f"\nImport complete: {inserted} inserted, {skipped} skipped, {failed} failed.")
    if failed > 0:
        sys.exit(1)


@app.command("migrate")
def migrate_command(
    ctx: typer.Context,
    database_url: str | None = typer.Option(
        None, "--database-url", help="PostgreSQL DSN"
    ),
) -> None:
    """Apply pending Alembic database migrations (upgrade head). Idempotent."""
    from paper_sorts.config import Settings

    if database_url:
        url = database_url
    else:
        engine: Engine | None = ctx.obj
        if engine is not None:
            url = str(engine.url)
        else:
            try:
                url = Settings().get_database_url()
            except ValueError as exc:
                print(f"Cannot determine database URL: {exc}")
                sys.exit(1)

    os.environ["PDBSEARCH_DATABASE_URL"] = url

    try:
        from alembic import command as alembic_cmd
        from alembic.config import Config

        cli_dir = Path(__file__).parent
        project_root = cli_dir.parent.parent.parent
        alembic_ini = project_root / "alembic.ini"

        if not alembic_ini.exists():
            print(f"alembic.ini not found at {alembic_ini}")
            sys.exit(1)

        alembic_cfg = Config(str(alembic_ini))
        db_hint = url.split("@")[-1] if "@" in url else url
        print(f"Applying migrations to: {db_hint}")
        alembic_cmd.upgrade(alembic_cfg, "head")
        print("Migrations applied successfully.")
        logger.info("Alembic upgrade head completed")
    except Exception as exc:  # noqa: BLE001
        print(f"Migration failed: {exc}")
        logger.error("Migration failed: %s", exc)
        sys.exit(1)
