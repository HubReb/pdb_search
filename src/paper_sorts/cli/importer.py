"""``import`` subcommand: bulk-import papers from a ``.tex`` + ``.bib`` pair.

Each extracted paper is committed individually (per-paper commit), so a partial
failure leaves earlier papers persisted and the command is re-runnable: a paper
whose BibTeX key already exists is skipped rather than re-inserted. Admin/scripted
— deliberately absent from the interactive menu.
"""

from __future__ import annotations

import logging
from pathlib import Path

from rich.console import Console
from sqlalchemy import Engine

from paper_sorts.db.repositories import DuplicateBibtexError
from paper_sorts.services import paper_service
from paper_sorts.services.import_service import extract_papers_from_tex_bib

_logger = logging.getLogger(__name__)
_console = Console()


def run_import(engine: Engine, tex_path: str, bib_path: str) -> None:
    """Import every cited paper from the given ``.tex`` and ``.bib`` files.

    :param engine: the database engine.
    :param tex_path: path to the LaTeX literature file.
    :param bib_path: path to the BibTeX references file.
    """
    tex = Path(tex_path).read_text(encoding="utf-8")
    bib = Path(bib_path).read_text(encoding="utf-8")

    added = 0
    skipped = 0
    for paper in extract_papers_from_tex_bib(tex, bib):
        try:
            paper_service.add_paper(engine, paper)
        except DuplicateBibtexError:
            _logger.info("Skipping already-present BibTeX key %r", paper.bibtex_id)
            skipped += 1
            continue
        added += 1
    _console.print(f"Imported {added} paper(s); skipped {skipped} already present.")
