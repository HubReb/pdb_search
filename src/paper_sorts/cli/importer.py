"""Typer `import` subcommand for paper_sorts.

Bulk-imports papers from a .tex + .bib file pair.
Per-paper commit semantics: a partial failure leaves prior entries intact.
This is a subcommand-only operation — NOT in the four-option interactive menu.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import typer
from sqlalchemy import Engine

from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service
from paper_sorts.services.import_service import extract_papers_from_tex_bib

logger = logging.getLogger("paper_sorts.cli.importer")

app = typer.Typer()


@app.command("import")
def import_command(
    ctx: typer.Context,
    tex: Path = typer.Option(..., "--tex", help="Path to the .tex file", exists=True),
    bib: Path = typer.Option(..., "--bib", help="Path to the .bib file", exists=True),
) -> None:
    """Bulk-import papers from a LaTeX + BibTeX file pair.

    Each paper is committed individually so a partial failure leaves
    previously imported entries intact.
    """
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

    for paper in papers:
        try:
            with with_session(engine) as session:
                paper_service.add_paper(session, paper)
            inserted += 1
            print(f"  Imported: {paper.title} (key: {paper.bibtex_id})")
        except ValueError as exc:
            skipped += 1
            logger.warning("Skipping '%s': %s", paper.title, exc)
            print(f"  Skipped: {paper.title} — {exc}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            logger.error("Failed to import '%s': %s", paper.title, exc)
            print(f"  Failed: {paper.title} — check logs for details")

    print(f"\nImport complete: {inserted} inserted, {skipped} skipped, {failed} failed.")
    if failed > 0:
        sys.exit(1)
