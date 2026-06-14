"""Integration tests for the paper_sorts persistence layer.

All tests run against a real ephemeral PostgreSQL instance provisioned by
pytest-postgresql from /usr/bin/pg_ctl.  No mocking of the SQLAlchemy session
or repositories is permitted (constitution Principle II).

Seed data: tests/fixtures/seed_papers.py — SEED_PAPERS.
"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from paper_sorts.db.repositories import (
    AuthorRepository,
    BibRepository,
    PaperCreate,
    PaperRepository,
    PaperSummary,
)

# ---------------------------------------------------------------------------
# search_by_title
# ---------------------------------------------------------------------------


def test_search_by_title_single_match(seeded_session: Session) -> None:
    """search_by_title returns one result for a uniquely-titled paper."""
    results = PaperRepository.search_by_title(
        seeded_session, "Direct speech-to-speech translation with discrete units"
    )
    assert len(results) == 1
    assert results[0].bibtex_id == "Lee2021DirectS2S"
    assert results[0].title == "Direct speech-to-speech translation with discrete units"
    assert "Lee, Ann" in results[0].authors


def test_search_by_title_multiple_matches(seeded_session: Session) -> None:
    """search_by_title returns multiple results when the same title exists twice."""
    results = PaperRepository.search_by_title(seeded_session, "Attention is all you need")
    assert len(results) == 2
    bibtex_ids = {r.bibtex_id for r in results}
    assert "Vaswani2017Attention" in bibtex_ids
    assert "Vaswani2017AttentionRepl" in bibtex_ids


def test_search_by_title_not_found(seeded_session: Session) -> None:
    """search_by_title returns an empty list for a non-existent title."""
    results = PaperRepository.search_by_title(seeded_session, "This title does not exist")
    assert results == []


# ---------------------------------------------------------------------------
# search_by_author
# ---------------------------------------------------------------------------


def test_search_by_author_found(seeded_session: Session) -> None:
    """search_by_author returns papers attributed to a known author."""
    results = PaperRepository.search_by_author(seeded_session, "Pino, J.")
    assert len(results) >= 1
    # "Pino, J." is an author of Wang2021LargeScaleSA in the seed data.
    bibtex_ids = {r.bibtex_id for r in results}
    assert "Wang2021LargeScaleSA" in bibtex_ids


def test_search_by_author_not_found(seeded_session: Session) -> None:
    """search_by_author returns an empty list for an unknown author."""
    results = PaperRepository.search_by_author(seeded_session, "NoSuchAuthor, X.")
    assert results == []


def test_search_by_author_multi_author_paper(seeded_session: Session) -> None:
    """search_by_author returns the correct paper for each co-author."""
    results_w = PaperRepository.search_by_author(seeded_session, "Wang, Changhan")
    results_p = PaperRepository.search_by_author(seeded_session, "Pino, J.")
    # Both co-authors should find the same paper.
    ids_w = {r.bibtex_id for r in results_w}
    ids_p = {r.bibtex_id for r in results_p}
    assert "Wang2021LargeScaleSA" in ids_w
    assert "Wang2021LargeScaleSA" in ids_p


# ---------------------------------------------------------------------------
# add_paper
# ---------------------------------------------------------------------------


def test_add_paper_success(db_session: Session) -> None:
    """add_paper inserts all four table rows correctly."""
    paper = PaperCreate(
        title="Test paper for add",
        contents="A one-sentence summary.",
        bibtex_id="TestAdd2024",
        bibtex="@misc{TestAdd2024, author={Test, Author}, title={Test paper for add}}",
        authors=["Test, Author", "Second, Writer"],
    )
    result = PaperRepository.add_paper(db_session, paper)
    db_session.commit()

    assert result.id is not None
    found = PaperRepository.search_by_title(db_session, "Test paper for add")
    assert len(found) == 1
    assert set(found[0].authors) == {"Test, Author", "Second, Writer"}
    assert found[0].bibtex_id == "TestAdd2024"


def test_add_paper_duplicate_bibtex_id_raises(db_session: Session) -> None:
    """add_paper raises IntegrityError if bibtex_id already exists."""
    paper = PaperCreate(
        title="Duplicate test",
        contents="Summary.",
        bibtex_id="DupTest2024",
        bibtex="@misc{DupTest2024, author={A, B}, title={Duplicate test}}",
        authors=["A, B"],
    )
    PaperRepository.add_paper(db_session, paper)
    db_session.commit()

    with pytest.raises(IntegrityError):
        PaperRepository.add_paper(db_session, paper)


# ---------------------------------------------------------------------------
# update_field (via repositories directly)
# ---------------------------------------------------------------------------


def test_update_paper_title(seeded_session: Session) -> None:
    """update_paper_field changes the title column."""
    paper = PaperRepository.search_by_title(seeded_session, "Updateable paper title")
    assert len(paper) == 1
    paper_id: int = paper[0].id

    PaperRepository.update_paper_field(seeded_session, paper_id, "title", "Updated title")
    seeded_session.commit()

    found = PaperRepository.search_by_title(seeded_session, "Updated title")
    assert len(found) == 1
    assert found[0].id == paper_id


def test_update_paper_contents(seeded_session: Session) -> None:
    """update_paper_field changes the contents column."""
    paper = PaperRepository.search_by_title(seeded_session, "Updateable paper title")
    assert len(paper) >= 1
    paper_id = paper[0].id

    PaperRepository.update_paper_field(seeded_session, paper_id, "contents", "New summary text.")
    seeded_session.commit()

    from sqlalchemy import select

    from paper_sorts.db.models import Paper
    row = seeded_session.scalar(select(Paper).where(Paper.id == paper_id))
    assert row is not None
    assert row.contents == "New summary text."


def test_update_bib(seeded_session: Session) -> None:
    """update_bib changes the bibtex field."""
    new_bibtex = "@misc{Lee2021DirectS2S, title={Updated}}"
    BibRepository.update_bib(seeded_session, "Lee2021DirectS2S", new_bibtex)
    seeded_session.commit()
    bib = BibRepository.get_bib(seeded_session, "Lee2021DirectS2S")
    assert bib is not None
    assert "Updated" in bib.bibtex


def test_update_bib_duplicate_bibtex_raises(seeded_session: Session) -> None:
    """update_bib raises ValueError if the new bibtex string is already stored."""
    # Get the existing bibtex string of the second paper.
    bib = BibRepository.get_bib(seeded_session, "Wang2021LargeScaleSA")
    assert bib is not None
    existing_bibtex = bib.bibtex

    with pytest.raises(ValueError, match="already stored"):
        BibRepository.update_bib(seeded_session, "Lee2021DirectS2S", existing_bibtex)


def test_update_author_rename(seeded_session: Session) -> None:
    """update_author_name renames an author and preserves paper links."""
    results_before = PaperRepository.search_by_author(seeded_session, "Lee, Ann")
    assert len(results_before) == 1

    AuthorRepository.update_author_name(seeded_session, "Lee, Ann", "Lee, Annie")
    seeded_session.commit()

    results_old = PaperRepository.search_by_author(seeded_session, "Lee, Ann")
    assert results_old == []

    results_new = PaperRepository.search_by_author(seeded_session, "Lee, Annie")
    assert len(results_new) == 1
    assert results_new[0].bibtex_id == "Lee2021DirectS2S"


# ---------------------------------------------------------------------------
# delete_paper
# ---------------------------------------------------------------------------


def test_delete_paper_removes_all_rows(seeded_session: Session) -> None:
    """delete_paper removes the paper, bib entry, and authorship rows."""
    # Verify the paper exists before deletion.
    before = PaperRepository.search_by_title(seeded_session, "Paper to be deleted")
    assert len(before) == 1

    PaperRepository.delete_paper(seeded_session, "Delete2024Test")
    seeded_session.commit()

    after = PaperRepository.search_by_title(seeded_session, "Paper to be deleted")
    assert after == []

    bib = BibRepository.get_bib(seeded_session, "Delete2024Test")
    assert bib is None

    # The author "Deleter, Bob" should be gone (orphan cleanup).
    author_results = PaperRepository.search_by_author(seeded_session, "Deleter, Bob")
    assert author_results == []


def test_delete_paper_nonexistent_raises(seeded_session: Session) -> None:
    """delete_paper raises ValueError for a non-existent bibtex_id."""
    with pytest.raises(ValueError, match="not found"):
        PaperRepository.delete_paper(seeded_session, "DoesNotExist999")


# ---------------------------------------------------------------------------
# PaperSummary DTO completeness
# ---------------------------------------------------------------------------


def test_paper_summary_has_all_fields(seeded_session: Session) -> None:
    """PaperSummary returned by search contains all required fields."""
    results = PaperRepository.search_by_title(
        seeded_session, "Large-scale Self- and Semi-Supervised learning for speech translation"
    )
    assert len(results) == 1
    p = results[0]
    assert isinstance(p, PaperSummary)
    assert p.id > 0
    assert p.title
    assert p.contents
    assert p.bibtex_id == "Wang2021LargeScaleSA"
    assert p.bibtex
    assert len(p.authors) == 2
