"""Bulk-import subcommand.

Consumes the per-paper iterator from the import service and persists each paper, committing per
paper so a partial failure leaves earlier inserts intact. Already-present BibTeX keys are
skipped (the key's uniqueness makes reruns safe).
"""

from __future__ import annotations

import logging
from pathlib import Path

from paper_sorts.cli import prompts
from paper_sorts.services.import_service import extract_papers_from_tex_bib
from paper_sorts.services.paper_service import PaperService

logger = logging.getLogger(__name__)


def run_import(service: PaperService, tex: Path, bib: Path) -> int:
    """Import every cited entry with a matching BibTeX record.

    :param service: the paper service to persist through.
    :param tex: path to the LaTeX literature-overview file.
    :param bib: path to the BibTeX file.
    :return: the number of papers inserted.
    """
    inserted = 0
    for paper in extract_papers_from_tex_bib(tex, bib):
        try:
            service.add_paper(paper)
            inserted += 1
        except ValueError as exc:
            logger.info("skipping %s: %s", paper.bibtex_id, exc)
    prompts.show(f"Imported {inserted} entr{'y' if inserted == 1 else 'ies'}.")
    return inserted
