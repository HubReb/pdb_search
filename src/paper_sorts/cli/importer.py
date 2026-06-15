"""The ``import`` command: bulk-import from a ``.tex`` + ``.bib`` pair."""

from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy import Engine

from paper_sorts.cli import prompts
from paper_sorts.db.repositories import PaperRepository
from paper_sorts.db.session import with_session
from paper_sorts.logging_config import LOGGER_NAME
from paper_sorts.services.import_service import extract_papers_from_tex_bib

logger = logging.getLogger(LOGGER_NAME)


def run_import(engine: Engine, tex_path: str, bib_path: str) -> int:
    """Import every cited+matched paper, committing each individually.

    A cited key with no ``.bib`` record is skipped (handled upstream). A paper
    whose BibTeX key already exists is skipped with a warning so a re-run is
    idempotent. Each successful paper is committed on its own, so a mid-import
    failure leaves earlier papers persisted (Principle IV / FR-005).

    :param engine: the database engine.
    :param tex_path: path to the LaTeX overview file.
    :param bib_path: path to the matching ``.bib`` file.
    :returns: the number of papers inserted.
    """
    tex = Path(tex_path).read_text(encoding="utf-8")
    bib = Path(bib_path).read_text(encoding="utf-8")
    inserted = 0
    for paper in extract_papers_from_tex_bib(tex, bib):
        try:
            with with_session(engine) as session:
                PaperRepository(session).add(paper)
            inserted += 1
        except ValueError as exc:
            logger.warning("Skipping %s: %s", paper.bibtex_id, exc)
    prompts.info(f"Imported {inserted} paper(s).")
    return inserted
