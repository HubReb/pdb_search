"""import subcommand for pdbsearch CLI.

Provides `pdbsearch import TEX_FILE BIB_FILE` for bulk import of papers
from a .tex + .bib pair.
Admin-only operation; not part of the interactive 4-option menu.
"""

from __future__ import annotations

import logging
from pathlib import Path

import typer

logger = logging.getLogger(__name__)

app = typer.Typer()


@app.command("import")
def import_cmd(
    ctx: typer.Context,
    tex_file: Path = typer.Argument(..., help="Path to the .tex file"),  # noqa: B008
    bib_file: Path = typer.Argument(..., help="Path to the .bib file"),  # noqa: B008
) -> None:
    """Bulk import papers from a LaTeX + BibTeX file pair.

    Each cited key in TEX_FILE that has a matching record in BIB_FILE is
    inserted as a new paper (per-paper transaction). Keys without a matching
    BibTeX record are skipped with a logged warning. Duplicate keys (already
    in the database) are also skipped.

    :param ctx: Typer context carrying settings from the app callback
    :param tex_file: path to the .tex file
    :param bib_file: path to the .bib file
    """
    from paper_sorts.services.import_service import extract_papers_from_tex_bib
    from paper_sorts.services.paper_service import add_paper

    settings = ctx.obj["settings"] if ctx.obj else None
    database_url: str
    if settings is not None:
        database_url = settings.get_database_url()
    else:
        raise typer.BadParameter("No database URL configured.")

    try:
        tex_content = tex_file.read_text(encoding="utf-8")
        bib_content = bib_file.read_text(encoding="utf-8")
    except OSError as exc:
        print(f"Could not read input files: {exc}")
        logger.error("File read error: %s", exc)
        raise typer.Exit(1) from exc

    imported = 0
    skipped = 0

    for paper_data in extract_papers_from_tex_bib(tex_content, bib_content):
        try:
            add_paper(database_url, paper_data)
            imported += 1
        except ValueError as exc:
            # Duplicate bibtex_id → already imported
            logger.info("Skipping '%s': %s", paper_data.bibtex_id, exc)
            skipped += 1
        except Exception as exc:
            logger.error(
                "Failed to import '%s': %s", paper_data.bibtex_id, exc
            )
            skipped += 1

    print(f"Imported {imported} papers, skipped {skipped}.")
