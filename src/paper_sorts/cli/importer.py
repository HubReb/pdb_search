"""The ``import`` subcommand: bulk-load papers from a ``.tex`` + ``.bib`` pair.

Each paper is committed individually so that a partial failure leaves earlier
papers persisted; rerunning skips already-present BibTeX keys (Constitution IV,
US5-3).
"""

from __future__ import annotations

from sqlalchemy import Engine

from paper_sorts.cli import prompts
from paper_sorts.db.repositories import DuplicateBibtexKeyError
from paper_sorts.logging_config import get_logger
from paper_sorts.services.import_service import extract_papers_from_tex_bib
from paper_sorts.services.paper_service import PaperService

logger = get_logger(__name__)


def run_import(engine: Engine, tex_path: str, bib_path: str) -> int:
    """Import all matched papers from a LaTeX overview and a BibTeX file.

    :param engine: the database engine.
    :param tex_path: path to the LaTeX literature-overview file.
    :param bib_path: path to the BibTeX file.
    :return: the number of papers newly inserted.
    """
    service = PaperService(engine)
    inserted = 0
    for paper in extract_papers_from_tex_bib(tex_path, bib_path):
        try:
            service.add_paper(paper)
            inserted += 1
        except DuplicateBibtexKeyError:
            logger.info("bibtex key %s already present — skipping", paper.bibtex_id)
    prompts.info(f"Imported {inserted} paper(s).")
    return inserted
