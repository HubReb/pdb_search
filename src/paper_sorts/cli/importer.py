"""Import subcommand: bulk-load papers from a ``.tex`` + ``.bib`` pair.

Each matched paper is committed individually (constitution Principle IV / US5
AS3), so a mid-run failure preserves already-imported papers. Existing BibTeX
keys are skipped, making reruns idempotent.
"""

from __future__ import annotations

from rich.console import Console
from sqlalchemy import Engine

from paper_sorts.db.repositories import DuplicateError
from paper_sorts.logging_config import get_logger
from paper_sorts.services.import_service import extract_papers_from_tex_bib
from paper_sorts.services.paper_service import PaperService

console = Console()
logger = get_logger()


def run_import(engine: Engine, tex_path: str, bib_path: str) -> int:
    """Import every matched paper from the ``.tex``/``.bib`` pair.

    :param engine: the engine bound to the configured database.
    :param tex_path: path to the LaTeX literature overview.
    :param bib_path: path to the matching ``.bib`` file.
    :returns: the number of papers inserted.
    """
    service = PaperService(engine)
    inserted = 0
    for paper in extract_papers_from_tex_bib(tex_path, bib_path):
        try:
            service.add_paper(paper)
            inserted += 1
            logger.info("imported %s", paper.bibtex_id)
        except DuplicateError:
            logger.warning("skipping already-present entry %s", paper.bibtex_id)
        except ValueError as exc:
            logger.warning("skipping %s: %s", paper.bibtex_id, exc)
    console.print(f"Imported {inserted} paper(s).")
    return inserted
