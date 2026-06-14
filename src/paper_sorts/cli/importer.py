"""Import subcommand for paper_sorts CLI.

Bulk-imports papers from a .tex + .bib file pair.
Not part of the interactive menu — subcommand only.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import typer
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from paper_sorts.db.session import with_session
from paper_sorts.services import import_service, paper_service

logger = logging.getLogger(__name__)

app = typer.Typer(help="Bulk-import papers from a .tex + .bib pair.")


def run_import(session: Session, tex_path: Path, bib_path: Path) -> None:
    """Execute the bulk import within a given session.

    :param session: Active SQLAlchemy session.
    :param tex_path: Path to the .tex file.
    :param bib_path: Path to the .bib file.
    """
    added = 0
    skipped = 0
    for paper_data in import_service.extract_papers_from_tex_bib(tex_path, bib_path):
        try:
            paper_service.add_paper(session, paper_data)
            added += 1
            typer.echo(f"  Imported: {paper_data.title}")
        except ValueError as exc:
            logger.warning("Skipping %r: %s", paper_data.bibtex_id, exc)
            skipped += 1
        except Exception as exc:
            logger.error("Failed to import %r: %s", paper_data.bibtex_id, exc)
            skipped += 1

    typer.echo(f"\nImport complete: {added} added, {skipped} skipped.")


@app.callback(invoke_without_command=True)
def importer(
    ctx: typer.Context,
    tex: Path | None = typer.Option(None, "--tex", help="Path to the .tex file"),
    bib: Path | None = typer.Option(None, "--bib", help="Path to the .bib file"),
) -> None:
    """Bulk-import papers from a LaTeX .tex + BibTeX .bib file pair."""
    if ctx.resilient_parsing:
        return

    if tex is None or bib is None:
        typer.echo("Error: --tex and --bib are required.", err=True)
        sys.exit(1)
    if not tex.exists():
        typer.echo(f"File not found: {tex}", err=True)
        sys.exit(1)
    if not bib.exists():
        typer.echo(f"File not found: {bib}", err=True)
        sys.exit(1)

    session: Session | None = ctx.obj.get("session") if ctx.obj else None
    if session is not None:
        try:
            run_import(session, tex, bib)
        except Exception as exc:
            logger.exception("Import failed: %s", exc)
            typer.echo(f"Import failed: {exc}", err=True)
            sys.exit(1)
        return

    engine: Engine | None = ctx.obj.get("engine") if ctx.obj else None
    if engine is None:
        logger.error("No database engine available")
        typer.echo("Error: database not configured.", err=True)
        sys.exit(1)
    try:
        with with_session(engine) as s:
            run_import(s, tex, bib)
    except Exception as exc:
        logger.exception("Import failed: %s", exc)
        typer.echo(f"Import failed: {exc}", err=True)
        sys.exit(1)
