"""Import subcommand for paper_sorts CLI.

Implements ``pdbsearch import``: bulk-imports papers from a LaTeX + BibTeX
file pair.  This is an admin-only subcommand — it is NOT shown in the
interactive top-level menu.
"""

from __future__ import annotations

import logging
from pathlib import Path

import typer
from rich.console import Console
from sqlalchemy.exc import IntegrityError

from paper_sorts.services import import_service, paper_service

logger = logging.getLogger(__name__)
console = Console()

app = typer.Typer(help="Bulk-import papers from .tex + .bib files (admin).")


@app.callback(invoke_without_command=True)
def import_cmd(
    ctx: typer.Context,
    tex: Path = typer.Option(..., help="Path to .tex file."),
    bib: Path = typer.Option(..., help="Path to .bib file."),
) -> None:
    """Import all cited papers from *tex* and *bib* into the database.

    Each paper is committed individually (per-paper commit), so a partial
    failure preserves already-inserted entries.  Duplicate BibTeX keys are
    skipped (idempotent re-runs).

    :param ctx: Typer context with ``engine`` in ``ctx.obj``.
    :param tex: Path to the ``.tex`` overview file.
    :param bib: Path to the ``.bib`` file.
    """
    if ctx.invoked_subcommand is not None:
        return
    engine = ctx.obj["engine"]

    if not tex.exists():
        console.print(f"[red]TeX file not found: {tex}[/red]")
        raise typer.Exit(code=1)
    if not bib.exists():
        console.print(f"[red]BibTeX file not found: {bib}[/red]")
        raise typer.Exit(code=1)

    inserted = 0
    skipped = 0

    for paper in import_service.extract_papers_from_tex_bib(tex, bib):
        try:
            paper_service.add_paper(engine, paper)
            inserted += 1
            logger.info("Imported paper %r", paper.bibtex_id)
        except IntegrityError:
            skipped += 1
            logger.info("Skipped duplicate paper %r", paper.bibtex_id)
        except Exception as exc:
            console.print(
                f"[red]Failed to import {paper.bibtex_id!r}: {exc}[/red]"
            )
            logger.exception("Failed to import %r: %s", paper.bibtex_id, exc)

    console.print(
        f"[green]Import complete: {inserted} inserted, {skipped} skipped.[/green]"
    )
