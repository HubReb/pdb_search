"""Import subcommand for pdbsearch (admin/scripted only).

Provides :func:`import_cmd` — a Typer command that bulk-imports papers from a
``.tex`` + ``.bib`` file pair.  Not shown in the interactive four-option menu.

Each paper is committed individually so a partial failure leaves earlier papers
persisted (constitution Principle IV).

Usage::

    pdbsearch import literature.tex bib.bib
"""

from __future__ import annotations

import logging

import typer

from paper_sorts.db.session import with_session
from paper_sorts.services import import_service, paper_service

logger = logging.getLogger(__name__)

app = typer.Typer(help="Bulk import papers from a .tex + .bib file pair (admin only).")


@app.callback(invoke_without_command=True)
def import_cmd(
    ctx: typer.Context,
    tex_file: str = typer.Argument(..., help="Path to the .tex file"),
    bib_file: str = typer.Argument(..., help="Path to the .bib file"),
    database_url: str = typer.Option(
        None, "--database-url", envvar="PDBSEARCH_DATABASE_URL", help="PostgreSQL DSN"
    ),
) -> None:
    """Bulk import papers cited in TEX_FILE with BibTeX data from BIB_FILE.

    Each paper is committed individually so a partial failure does not roll
    back previously imported entries.

    :param tex_file: Path to the ``.tex`` literature overview file.
    :param bib_file: Path to the ``.bib`` bibliography file.
    :param database_url: PostgreSQL DSN.
    """
    if ctx.invoked_subcommand is not None:
        return
    if not database_url:
        typer.echo("Error: database URL not configured.", err=True)
        raise typer.Exit(1)

    typer.echo(f"Importing from {tex_file!r} + {bib_file!r}...")
    imported = 0
    skipped = 0

    try:
        papers = import_service.extract_papers_from_tex_bib(tex_file, bib_file)
    except (FileNotFoundError, OSError) as exc:
        logger.error("Failed to read import files: %s", exc)
        typer.echo(f"Error reading files: {exc}", err=True)
        raise typer.Exit(1) from exc

    for paper in papers:
        try:
            paper_service.add_paper(
                paper, database_url=database_url, with_session_fn=with_session
            )
            imported += 1
            logger.debug("Imported: %r", paper.bibtex_key)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Skipped %r: %s", paper.bibtex_key, exc)
            skipped += 1

    typer.echo(f"Done. Imported: {imported}, skipped: {skipped}.")
