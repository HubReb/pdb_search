"""Import subcommand for pdbsearch (admin-only, not in interactive menu).

Registered as ``pdbsearch import`` in :mod:`paper_sorts.cli.app`.
Bulk-imports papers from a LaTeX overview file + BibTeX file.
"""

from __future__ import annotations

import logging

import typer

logger = logging.getLogger(__name__)

app = typer.Typer(help="Bulk-import papers from a LaTeX + BibTeX file pair (admin operation).")


@app.callback(invoke_without_command=True)
def import_cmd(
    ctx: typer.Context,
    tex: str = typer.Option(..., "--tex", help="Path to the .tex overview file"),
    bib: str = typer.Option(..., "--bib", help="Path to the .bib file"),
) -> None:
    """Import all cited papers from *tex* + *bib* into the database.

    Each paper is committed individually so a partial failure leaves already-
    inserted entries intact.  Missing BibTeX records are skipped with a
    warning.

    :param ctx: Typer context carrying the SQLAlchemy engine.
    :param tex: Path to the LaTeX overview file.
    :param bib: Path to the BibTeX file.
    """
    engine = ctx.obj["engine"]
    from paper_sorts.services import import_service, paper_service

    inserted = 0
    skipped = 0

    for paper in import_service.extract_papers_from_tex_bib(tex, bib):
        try:
            paper_service.add_paper(engine, paper)
            inserted += 1
        except ValueError as exc:
            logger.warning("Skipped %r: %s", paper.bibtex_id, exc)
            skipped += 1

    print(f"Import complete: {inserted} added, {skipped} skipped.")
