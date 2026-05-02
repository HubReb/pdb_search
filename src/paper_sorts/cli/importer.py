"""``pdbsearch import`` — bulk import papers from a ``.tex`` + ``.bib`` pair.

Reachable as a Typer subcommand only — *not* from the top-level menu —
because bulk import is a deliberate scripted invocation (the legacy code
exposed it as ``python paper_sorts/get_data.py ...``, never through
``UserInteraction``). See ``contracts/cli-commands.md`` § "Why only four
options".

Each yielded :class:`PaperCreate` runs in its own
:func:`with_session` transaction (constitution Principle IV bulk-import
rule): a partial failure leaves the database in a state recoverable on
rerun. Re-running is idempotent — BibTeX keys already in the database
raise :class:`DuplicateBibtexIdError`, which the loop logs INFO and
skips, mirroring the legacy ``add_data_from_dict`` "skip if exists"
behaviour. A summary line at the end reports inserted / skipped /
warned counts; the warned count is collected by attaching a counting
handler to the :mod:`paper_sorts.services.import_service` logger for
the duration of the run (the iterator itself returns ``Iterator[PaperCreate]``,
so the count can't ride on the yield).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated

import typer

from paper_sorts.db.repositories import DuplicateBibtexIdError
from paper_sorts.db.session import with_session
from paper_sorts.services import import_service
from paper_sorts.services.import_service import extract_papers_from_tex_bib
from paper_sorts.services.paper_service import PaperService


class _WarningCounter(logging.Handler):
    """Count WARNING records emitted on the import-service logger."""

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.count = 0

    def emit(self, record: logging.LogRecord) -> None:
        if record.levelno >= logging.WARNING:
            self.count += 1


def import_(
    ctx: typer.Context,
    tex_file: Annotated[
        Path,
        typer.Argument(
            help="Path to the .tex literature overview citing the bib entries.",
            exists=True,
            dir_okay=False,
            readable=True,
        ),
    ],
    bib_file: Annotated[
        Path,
        typer.Argument(
            help="Path to the .bib source containing the cited entries.",
            exists=True,
            dir_okay=False,
            readable=True,
        ),
    ],
) -> None:
    """Import papers from a ``.tex`` + ``.bib`` pair, one transaction per paper."""
    factory = ctx.obj
    inserted = 0
    skipped = 0

    counter = _WarningCounter()
    service_logger = logging.getLogger(import_service.__name__)
    service_logger.addHandler(counter)
    try:
        for payload in extract_papers_from_tex_bib(tex_file, bib_file):
            try:
                with with_session(factory) as session:
                    PaperService(session).add_paper(payload)
            except DuplicateBibtexIdError:
                logging.getLogger(__name__).info(
                    "BibTeX key %r already in database; skipping.",
                    payload.bibtex_id,
                )
                skipped += 1
                continue
            inserted += 1
    finally:
        service_logger.removeHandler(counter)

    print(f"Import complete: inserted={inserted}, skipped={skipped}, warned={counter.count}.")
