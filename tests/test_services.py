"""Tests for the paper_sorts service layer.

Integration tests using the ephemeral DB session (no mocking — Principle II).
"""

from __future__ import annotations

import pytest

from paper_sorts.db.repositories import PaperCreate
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service
from tests.fixtures.seed_papers import SEED_PAPERS


def test_search_by_title_one_match(seeded_db_url: str) -> None:
    """search_by_title returns exactly one result for a unique title substring."""
    with with_session(seeded_db_url) as session:
        results = paper_service.search_by_title(session, "Large-Scale")
    assert len(results) == 1
    assert results[0].bibtex_id == "Wang2021LargeScaleSA"


def test_search_by_title_multiple(seeded_db_url: str) -> None:
    """search_by_title returns multiple papers for a shared title substring."""
    with with_session(seeded_db_url) as session:
        results = paper_service.search_by_title(session, "Attention Is All You Need")
    assert len(results) == 2


def test_search_by_author(seeded_db_url: str) -> None:
    """search_by_author finds papers by partial author name match."""
    with with_session(seeded_db_url) as session:
        results = paper_service.search_by_author(session, "He, Kaiming")
    assert any(r.bibtex_id == "He2016DeepRL" for r in results)


def test_search_by_title_no_results(seeded_db_url: str) -> None:
    """search_by_title returns empty list for non-existent title."""
    with with_session(seeded_db_url) as session:
        results = paper_service.search_by_title(session, "ZzzzNotInDatabase")
    assert results == []


def test_add_paper(db_url: str) -> None:
    """add_paper inserts a paper and returns a PaperSummary."""
    paper = SEED_PAPERS[2]
    with with_session(db_url) as session:
        result = paper_service.add_paper(session, paper)
    assert result.bibtex_id == "Wang2021LargeScaleSA"
    assert result.id > 0


def test_add_paper_empty_title_raises(db_url: str) -> None:
    """add_paper raises ValueError for empty title."""
    paper = PaperCreate(
        title="",
        contents="Some content",
        bibtex_id="test123",
        bibtex="@article{test123}",
        authors=["Smith, John"],
    )
    with pytest.raises(ValueError, match="title"), with_session(db_url) as session:
        paper_service.add_paper(session, paper)


def test_add_paper_empty_bibtex_id_raises(db_url: str) -> None:
    """add_paper raises ValueError for empty bibtex_id."""
    paper = PaperCreate(
        title="Some Title",
        contents="Some content",
        bibtex_id="",
        bibtex="@article{test123}",
        authors=["Smith, John"],
    )
    with pytest.raises(ValueError, match="bibtex_id"), with_session(db_url) as session:
        paper_service.add_paper(session, paper)


def test_add_paper_no_authors_raises(db_url: str) -> None:
    """add_paper raises ValueError when no authors provided."""
    paper = PaperCreate(
        title="Some Title",
        contents="Some content",
        bibtex_id="test123",
        bibtex="@article{test123}",
        authors=[],
    )
    with pytest.raises(ValueError, match="author"), with_session(db_url) as session:
        paper_service.add_paper(session, paper)


def test_update_field_title(seeded_db_url: str) -> None:
    """update_field 'title' persists the new title."""
    with with_session(seeded_db_url) as session:
        results = paper_service.search_by_title(session, "Large-Scale")
    paper_id = results[0].id

    with with_session(seeded_db_url) as session:
        updated = paper_service.update_field(session, paper_id, "title", "New Title")
    assert updated.title == "New Title"


def test_update_field_contents(seeded_db_url: str) -> None:
    """update_field 'contents' persists the new summary."""
    with with_session(seeded_db_url) as session:
        results = paper_service.search_by_title(session, "Large-Scale")
    paper_id = results[0].id

    with with_session(seeded_db_url) as session:
        updated = paper_service.update_field(session, paper_id, "contents", "New summary")
    assert updated.contents == "New summary"


def test_update_field_bibtex(seeded_db_url: str) -> None:
    """update_field 'bibtex' persists the new BibTeX string."""
    with with_session(seeded_db_url) as session:
        results = paper_service.search_by_title(session, "Large-Scale")
    paper_id = results[0].id

    new_bib = "@article{NewKey2024, title={New}}"
    with with_session(seeded_db_url) as session:
        updated = paper_service.update_field(session, paper_id, "bibtex", new_bib)
    assert updated.bibtex == new_bib


def test_update_field_authors(seeded_db_url: str) -> None:
    """update_field 'authors' replaces the author list."""
    with with_session(seeded_db_url) as session:
        results = paper_service.search_by_title(session, "Large-Scale")
    paper_id = results[0].id

    with with_session(seeded_db_url) as session:
        updated = paper_service.update_field(session, paper_id, "authors", ["New, Author"])
    assert updated.authors == ["New, Author"]


def test_update_field_invalid_exhaustiveness() -> None:
    """update_field with an invalid field name raises TypeError via assert_never."""
    # We cannot actually pass an invalid Literal at type-check time, but we can
    # call with a runtime cast to verify the assert_never path is exercised.
    # This test documents the exhaustiveness contract.
    from typing import cast

    # Intentionally nested: pytest.raises must wrap with_session to catch the exception.
    # noqa: SIM117 — these cannot be combined; the raises context must be outer.
    with pytest.raises((TypeError, AssertionError)):  # noqa: SIM117
        with with_session("postgresql+psycopg://invalid") as session:
            paper_service.update_field(
                session, 1, cast(paper_service.UpdateableField, "nonexistent_field"), "x"
            )


def test_delete_paper(seeded_db_url: str) -> None:
    """delete_paper removes the paper from the database."""
    with with_session(seeded_db_url) as session:
        results = paper_service.search_by_title(session, "Large-Scale")
    paper_id = results[0].id

    with with_session(seeded_db_url) as session:
        paper_service.delete_paper(session, paper_id)

    with with_session(seeded_db_url) as session:
        results_after = paper_service.search_by_title(session, "Large-Scale")
    assert results_after == []


def test_delete_paper_not_found(seeded_db_url: str) -> None:
    """delete_paper raises ValueError for non-existent paper id."""
    with pytest.raises(ValueError, match="No paper with id"):  # noqa: SIM117
        with with_session(seeded_db_url) as session:
            paper_service.delete_paper(session, 99999)
