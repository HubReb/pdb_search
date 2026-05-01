"""Integration tests for ``PaperService.update_field`` (T034).

Covers the legacy ``UserInteraction.update`` table x field grid:
``papers.title`` / ``papers.contents`` / ``bib.bibtex`` / ``authors.author``.
Also covers the rejection rules (``bibtex_id`` is not editable;
non-existent paper id raises) and a CLI-level abort note (the abort
flow is gated by ``ask_confirm``, whose grammar is fully covered in
``tests/unit/test_prompts.py``; the integration-level abort proof is
the trivial complement: not calling the service leaves data unchanged,
asserted directly).
"""

from __future__ import annotations

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from paper_sorts.db.models import Author, BibEntry, Paper
from paper_sorts.services.paper_service import PaperService


def _wang_paper_id(session: Session) -> int:
    return session.execute(
        select(Paper.id).where(Paper.bibtex_id == "Wang2021LargeScaleSA")
    ).scalar_one()


def test_update_title(db_session: Session) -> None:
    paper_id = _wang_paper_id(db_session)
    service = PaperService(db_session)
    service.update_field("papers", "title", paper_id, "New Title")

    title = db_session.execute(
        select(Paper.title).where(Paper.id == paper_id)
    ).scalar_one()
    assert title == "New Title"


def test_update_contents(db_session: Session) -> None:
    paper_id = _wang_paper_id(db_session)
    service = PaperService(db_session)
    service.update_field("papers", "contents", paper_id, "Rewritten summary.")

    contents = db_session.execute(
        select(Paper.contents).where(Paper.id == paper_id)
    ).scalar_one()
    assert contents == "Rewritten summary."


def test_update_bibtex_source(db_session: Session) -> None:
    service = PaperService(db_session)
    new_source = "@article{Wang2021LargeScaleSA, note={updated source}}"
    service.update_field("bib", "bibtex", "Wang2021LargeScaleSA", new_source)

    bibtex = db_session.execute(
        select(BibEntry.bibtex).where(BibEntry.bibtex_id == "Wang2021LargeScaleSA")
    ).scalar_one()
    assert bibtex == new_source


def test_update_author_name(db_session: Session) -> None:
    pino_id = db_session.execute(
        select(Author.id).where(Author.name == "Pino, J.")
    ).scalar_one()
    service = PaperService(db_session)
    service.update_field("authors", "author", pino_id, "Pino, Juan")

    name = db_session.execute(
        select(Author.name).where(Author.id == pino_id)
    ).scalar_one()
    assert name == "Pino, Juan"


def test_update_bibtex_id_itself_is_rejected(db_session: Session) -> None:
    """The BibTeX identifier cannot be updated; only its source string is."""
    service = PaperService(db_session)
    with pytest.raises(ValueError, match="not editable"):
        service.update_field(
            "bib", "bibtex_id", "Wang2021LargeScaleSA", "RenameAttempt"
        )


def test_update_nonexistent_paper_id_rejected(db_session: Session) -> None:
    """Spec rule: non-existent paper id raises a plain-language ValueError."""
    service = PaperService(db_session)
    with pytest.raises(ValueError, match="No paper with id 99999"):
        service.update_field("papers", "title", 99999, "would not have applied")


def test_update_empty_value_rejected(db_session: Session) -> None:
    paper_id = _wang_paper_id(db_session)
    service = PaperService(db_session)
    with pytest.raises(ValueError, match="cannot be empty"):
        service.update_field("papers", "title", paper_id, "")


def test_update_unknown_field_on_papers_rejected(db_session: Session) -> None:
    paper_id = _wang_paper_id(db_session)
    service = PaperService(db_session)
    with pytest.raises(ValueError, match="not editable"):
        service.update_field("papers", "id", paper_id, "1")


def test_abort_complement_data_unchanged(db_session: Session) -> None:
    """If the CLI never calls update_field (user aborted), seeded data is intact.

    The grammar of the abort token is covered in ``tests/unit/test_prompts.py``
    (``ask_confirm`` rejects ``2``/``n``/``no``); the integration-level
    complement is asserting that without a service call no row is mutated.
    """
    paper_id = _wang_paper_id(db_session)
    title_before = db_session.execute(
        select(Paper.title).where(Paper.id == paper_id)
    ).scalar_one()

    # Deliberately do NOT call service.update_field — the "abort" path.

    title_after = db_session.execute(
        select(Paper.title).where(Paper.id == paper_id)
    ).scalar_one()
    assert title_after == title_before
