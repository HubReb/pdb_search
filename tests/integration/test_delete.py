"""Integration tests for ``PaperService.delete_paper`` (T035).

Covers the cascade rules from ``data-model.md``: ``authors_papers`` rows
gone, orphan author rows gone, bib row dropped only when no other paper
references it, non-existent id rejected with a plain-language error.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from paper_sorts.db.models import Author, Authorship, BibEntry, Paper
from paper_sorts.db.repositories import PaperCreate, PaperRepository
from paper_sorts.services.paper_service import PaperService


def _paper_id_by_bibtex(session: Session, bibtex_id: str) -> int:
    return session.execute(
        select(Paper.id).where(Paper.bibtex_id == bibtex_id)
    ).scalar_one()


def test_delete_drops_paper_and_its_authorship_links(db_session: Session) -> None:
    paper_id = _paper_id_by_bibtex(db_session, "Schoettler2023FairnessMT")
    service = PaperService(db_session)
    service.delete_paper(paper_id)

    paper = db_session.execute(
        select(Paper).where(Paper.id == paper_id)
    ).scalar_one_or_none()
    assert paper is None

    links = db_session.execute(
        select(Authorship).where(Authorship.paper_id == paper_id)
    ).scalars().all()
    assert links == []


def test_delete_removes_orphan_authors(db_session: Session) -> None:
    """Schöttler appears on exactly one paper; deleting it orphans the author row."""
    paper_id = _paper_id_by_bibtex(db_session, "Schoettler2023FairnessMT")
    schoettler = db_session.execute(
        select(Author).where(Author.name == "Schöttler, K.")
    ).scalar_one()
    schoettler_id = schoettler.id

    PaperService(db_session).delete_paper(paper_id)

    still_there = db_session.execute(
        select(Author).where(Author.id == schoettler_id)
    ).scalar_one_or_none()
    assert still_there is None


def test_delete_keeps_authors_with_other_papers(db_session: Session) -> None:
    """Pino appears on two papers; deleting one keeps the author row alive."""
    paper_id = _paper_id_by_bibtex(db_session, "Lee2022DirectSpeechToSpeech")
    PaperService(db_session).delete_paper(paper_id)

    pino = db_session.execute(
        select(Author).where(Author.name == "Pino, J.")
    ).scalar_one_or_none()
    assert pino is not None


def test_delete_drops_bib_row_when_no_other_paper_references_it(
    db_session: Session,
) -> None:
    paper_id = _paper_id_by_bibtex(db_session, "Schoettler2023FairnessMT")
    PaperService(db_session).delete_paper(paper_id)

    bib = db_session.execute(
        select(BibEntry).where(BibEntry.bibtex_id == "Schoettler2023FairnessMT")
    ).scalar_one_or_none()
    assert bib is None


def test_delete_keeps_bib_row_when_another_paper_references_it(
    db_session: Session,
) -> None:
    """Two papers sharing a bibtex_id is unusual but the cascade rule covers it."""
    # First add a second paper that points at the same bibtex_id as Wang2021.
    # The bib row already exists (seeded), so we add only paper + authorship —
    # the repository layer would normally reject duplicate bibtex_id at the
    # service boundary, so we drop down to the ORM directly to set up the
    # double-reference state. This bypasses PaperService.add_paper's
    # duplicate-key check intentionally for the test fixture.
    wang_paper = db_session.execute(
        select(Paper).where(Paper.bibtex_id == "Wang2021LargeScaleSA")
    ).scalar_one()
    second_paper = Paper(
        title="Second copy referencing the same bib row",
        contents="Same bibtex_id, different paper row.",
        bibtex_id=wang_paper.bibtex_id,
    )
    db_session.add(second_paper)
    db_session.flush()

    PaperService(db_session).delete_paper(wang_paper.id)

    bib = db_session.execute(
        select(BibEntry).where(BibEntry.bibtex_id == "Wang2021LargeScaleSA")
    ).scalar_one_or_none()
    assert bib is not None  # second_paper still references it


def test_delete_nonexistent_paper_id_rejected(db_session: Session) -> None:
    service = PaperService(db_session)
    with pytest.raises(ValueError, match="No paper with id 99999"):
        service.delete_paper(99999)


def test_delete_inserts_new_paper_then_round_trip(db_session: Session) -> None:
    """Smoke: full add -> delete round-trip exercises both directions of the seam."""
    payload = PaperCreate(
        title="Round Trip",
        contents="One-line summary.",
        bibtex_id="RoundTrip2026",
        bibtex="@misc{RoundTrip2026, year={2026}}",
        authors=("Solo, A.",),
    )
    inserted = PaperRepository(db_session).add(payload)
    db_session.flush()

    PaperService(db_session).delete_paper(inserted.id)

    assert (
        db_session.execute(
            select(Paper).where(Paper.id == inserted.id)
        ).scalar_one_or_none()
        is None
    )
    assert (
        db_session.execute(
            select(BibEntry).where(BibEntry.bibtex_id == "RoundTrip2026")
        ).scalar_one_or_none()
        is None
    )
    assert (
        db_session.execute(
            select(Author).where(Author.name == "Solo, A.")
        ).scalar_one_or_none()
        is None
    )
