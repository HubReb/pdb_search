"""Import subcommand for pdbsearch CLI.

Bulk-imports papers from a LaTeX .tex file and a matching .bib file.
Commits per-paper so a partial failure leaves prior entries intact.
"""

import logging
import sys
from pathlib import Path

import typer

from paper_sorts.services import import_service, paper_service

logger = logging.getLogger(__name__)

app = typer.Typer(help="Bulk import papers from a .tex + .bib pair.")


@app.command()
def import_cmd(
    ctx: typer.Context,
    tex: Path = typer.Option(..., "--tex", help="Path to the LaTeX literature-overview .tex file."),
    bib: Path = typer.Option(..., "--bib", help="Path to the BibTeX .bib file."),
) -> None:
    """Import all entries from the .tex/.bib pair into the database.

    Skips entries already present (idempotent by bibtex_id).
    Commits per-paper so a partial failure leaves prior entries intact.

    :param ctx: Typer context carrying the engine.
    :param tex: Path to the LaTeX file.
    :param bib: Path to the BibTeX file.
    """
    engine = ctx.obj.get("engine") if ctx.obj else None
    if engine is None:
        logger.error("No database connection available")
        print("Error: no database URL configured.")
        sys.exit(1)

    if not tex.exists():
        print(f"Error: .tex file not found: {tex}")
        sys.exit(1)
    if not bib.exists():
        print(f"Error: .bib file not found: {bib}")
        sys.exit(1)

    inserted = 0
    skipped = 0
    failed = 0

    print(f"Importing from {tex} + {bib}...")

    for paper in import_service.extract_papers_from_tex_bib(tex, bib):
        try:
            paper_service.add_paper(engine, paper)
            inserted += 1
            logger.info("Imported '%s' (%s)", paper.title, paper.bibtex_id)
        except ValueError as exc:
            # Duplicate bibtex_id — skip silently
            skipped += 1
            logger.info("Skipped '%s': %s", paper.bibtex_id, exc)
        except Exception as exc:
            failed += 1
            logger.error("Failed to import '%s': %s", paper.bibtex_id, exc)
            print(f"  Warning: failed to import '{paper.bibtex_id}': {exc}")

    print(
        f"Import complete: {inserted} inserted, {skipped} skipped (duplicate), "
        f"{failed} failed."
    )
