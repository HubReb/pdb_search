"""CLI 'import' subcommand for paper_sorts.

Batch imports papers from a LaTeX .tex + .bib file pair.
Non-interactive. Commits per paper (constitution Principle IV).
"""

from __future__ import annotations

import logging
from pathlib import Path

import typer

from paper_sorts.db.session import with_session
from paper_sorts.services import import_service, paper_service

logger = logging.getLogger(__name__)

app = typer.Typer()


def run_import(db_url: str, tex_path: Path, bib_path: Path) -> None:
    """Execute the bulk import from a .tex + .bib file pair.

    :param db_url: SQLAlchemy-compatible database URL.
    :param tex_path: Path to the LaTeX .tex file.
    :param bib_path: Path to the BibTeX .bib file.
    """
    if not tex_path.exists():
        typer.echo(f"LaTeX file not found: {tex_path}", err=True)
        raise typer.Exit(code=1)
    if not bib_path.exists():
        typer.echo(f"BibTeX file not found: {bib_path}", err=True)
        raise typer.Exit(code=1)

    typer.echo(f"Importing from '{tex_path}' + '{bib_path}'...")
    added = 0
    skipped = 0

    for paper in import_service.extract_papers_from_tex_bib(tex_path, bib_path):
        # Per-paper commit: each paper gets its own with_session call
        with with_session(db_url) as session:
            success = paper_service.add_paper(session, paper)
        if success:
            added += 1
        else:
            skipped += 1

    typer.echo(f"Import complete: {added} added, {skipped} skipped (already present or error).")


@app.command("import")
def importer(
    ctx: typer.Context,
    tex: Path = typer.Option(..., "--tex", help="Path to .tex file."),  # noqa: B008
    bib: Path = typer.Option(..., "--bib", help="Path to .bib file."),  # noqa: B008
) -> None:
    """Bulk import papers from a LaTeX .tex + .bib file pair."""
    db_url: str = ctx.obj["db_url"]
    run_import(db_url, tex, bib)
