"""Bulk ``import`` flow (presentation layer, subcommand-only)."""

from __future__ import annotations

import logging
from pathlib import Path

from paper_sorts.cli import prompts
from paper_sorts.services.import_service import extract_papers_from_tex_bib
from paper_sorts.services.paper_service import PaperService

_logger = logging.getLogger(__name__)


def run_import(service: PaperService, tex_path: Path, bib_path: Path) -> int:
    """Import every cited entry with a matching bib record, committing per paper.

    :param service: the bound paper service.
    :param tex_path: the LaTeX literature overview.
    :param bib_path: the BibTeX file.
    :returns: the number of papers inserted.
    """
    tex = tex_path.read_text(encoding="utf-8")
    bib = bib_path.read_text(encoding="utf-8")
    inserted = 0
    for paper in extract_papers_from_tex_bib(tex, bib):
        try:
            service.add_paper(paper)
            inserted += 1
        except ValueError as exc:
            _logger.warning("skipping %s: %s", paper.bibtex_id, exc)
    prompts.info(f"Imported {inserted} paper(s).")
    return inserted
