"""Import subcommand for pdbsearch CLI.

Non-interactive bulk import from a LaTeX literature overview + BibTeX pair.
"""

from __future__ import annotations

import logging
from pathlib import Path

from rich.console import Console

from paper_sorts.services import import_service, paper_service

logger = logging.getLogger(__name__)
console = Console()


def import_callback(db_url: str, tex: Path, bib: Path) -> None:
    """Bulk-import papers from a .tex + .bib file pair.

    Reads both files, extracts paper metadata, and inserts each entry
    into the database with per-paper commit semantics (constitution Principle IV).
    Entries that fail are logged and skipped — the import continues.

    Args:
        db_url: SQLAlchemy-compatible database URL from the app callback.
        tex: Path to the LaTeX literature overview file.
        bib: Path to the BibTeX bibliography file.
    """
    tex_content = tex.read_text(encoding="utf-8")
    bib_content = bib.read_text(encoding="utf-8")

    inserted = 0
    skipped = 0

    for paper in import_service.extract_papers_from_tex_bib(tex_content, bib_content):
        try:
            paper_service.add_paper(db_url, paper)
            inserted += 1
            logger.info("Imported paper %r", paper.title)
        except ValueError as exc:
            # Per-paper commit: skip duplicates and integrity errors, log them
            logger.warning("Skipping paper %r: %s", paper.title, exc)
            skipped += 1

    console.print(
        f"[green]Import complete: {inserted} paper(s) inserted, "
        f"{skipped} skipped.[/green]"
    )
