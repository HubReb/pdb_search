"""Bulk import subcommand for paper_sorts CLI.

This subcommand is admin-only and does not appear in the interactive top-level
menu.  It must be invoked directly: ``pdbsearch import --tex FILE --bib FILE``.
"""

from __future__ import annotations

import logging
from pathlib import Path

import typer

from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service
from paper_sorts.services.import_service import extract_papers_from_tex_bib

log = logging.getLogger(__name__)

app = typer.Typer(help="Bulk import papers from a LaTeX + BibTeX file pair.")


@app.command("import")
def import_papers(
    tex: Path = typer.Option(..., "--tex", help="Path to the .tex literature overview file."),
    bib: Path = typer.Option(..., "--bib", help="Path to the .bib BibTeX file."),
    database_url: str = typer.Option("", "--database-url", envvar="PDBSEARCH_DATABASE_URL"),
) -> None:
    """Import all papers cited in TEX that have a matching entry in BIB.

    Each paper is committed individually so that a partial failure leaves the
    database in a consistent state recoverable on rerun.

    :param tex: path to the ``.tex`` file.
    :param bib: path to the ``.bib`` file.
    :param database_url: SQLAlchemy database URL.
    """
    if not database_url:
        typer.echo("Error: database URL is required. Set --database-url or PDBSEARCH_DATABASE_URL.")
        raise typer.Exit(code=1)

    inserted = 0
    skipped = 0

    for paper_create in extract_papers_from_tex_bib(tex, bib):
        try:
            with with_session(database_url) as session:
                paper_service.add_paper(session, paper_create)
            typer.echo(f"  Imported: {paper_create.title}")
            inserted += 1
        except ValueError as exc:
            typer.echo(f"  Skipped '{paper_create.title}': {exc}")
            log.warning("Skipped '%s': %s", paper_create.title, exc)
            skipped += 1

    typer.echo(f"\nImport complete: {inserted} inserted, {skipped} skipped.")
