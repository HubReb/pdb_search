"""Integration tests for the service layer.

Tests call service functions with the real ephemeral DB session factory.
No mocking of SQLAlchemy session, repositories, or driver (constitution
Principle II).

Seed data is defined in ``tests/fixtures/seed_papers.py``.
"""

from __future__ import annotations

import pytest

from paper_sorts.db.repositories import PaperCreate, PaperRepository
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service
from tests.conftest import _cleanup_paper_and_bib
from tests.fixtures.seed_papers import SEED_PAPERS


@pytest.fixture(autouse=False)
def seeded_url(migrated_db_url: str) -> str:
    """Insert seed papers, yield the DB URL, then clean up.

    :param migrated_db_url: URL from the migrated ephemeral DB.
    :returns: The database URL string.
    """
    inserted_ids: list[int] = []
    with with_session(migrated_db_url) as s:
        for paper_data in SEED_PAPERS:
            summary = PaperRepository.create(s, paper_data)
            inserted_ids.append(summary.paper_id)

    yield migrated_db_url  # type: ignore[misc]

    _cleanup_paper_and_bib(migrated_db_url, inserted_ids)


# ---------------------------------------------------------------------------
# search_by_title
# ---------------------------------------------------------------------------


def test_search_by_title_found(seeded_url: str) -> None:
    """search_by_title returns results for a known title."""
    results = paper_service.search_by_title(
        "Direct speech-to-speech translation with discrete units",
        database_url=seeded_url,
        with_session_fn=with_session,
    )
    assert len(results) == 1
    assert results[0].bibtex_key == "Lee2022DirectSpeech"


def test_search_by_title_not_found(seeded_url: str) -> None:
    """search_by_title returns empty list for unknown title."""
    results = paper_service.search_by_title(
        "This title does not exist",
        database_url=seeded_url,
        with_session_fn=with_session,
    )
    assert results == []


# ---------------------------------------------------------------------------
# search_by_author
# ---------------------------------------------------------------------------


def test_search_by_author_found(seeded_url: str) -> None:
    """search_by_author returns papers for a known author."""
    results = paper_service.search_by_author(
        "Lee, Ann",
        database_url=seeded_url,
        with_session_fn=with_session,
    )
    assert len(results) >= 1
    keys = {r.bibtex_key for r in results}
    assert "Lee2022DirectSpeech" in keys


def test_search_by_author_not_found(seeded_url: str) -> None:
    """search_by_author returns empty list for unknown author."""
    results = paper_service.search_by_author(
        "Nobody, X.",
        database_url=seeded_url,
        with_session_fn=with_session,
    )
    assert results == []


# ---------------------------------------------------------------------------
# add_paper
# ---------------------------------------------------------------------------


def test_add_paper(migrated_db_url: str) -> None:
    """add_paper inserts a paper and it can be retrieved by title."""
    data = PaperCreate(
        title="Service Layer Add Test",
        authors=["Service, Author"],
        bibtex_key="ServiceAddTest2026",
        summary="A service-layer add test.",
        bibtex_text="@article{ServiceAddTest2026}",
    )
    result = paper_service.add_paper(
        data, database_url=migrated_db_url, with_session_fn=with_session
    )
    assert result.title == "Service Layer Add Test"
    assert result.paper_id > 0

    # Verify it's retrievable
    found = paper_service.search_by_title(
        "Service Layer Add Test",
        database_url=migrated_db_url,
        with_session_fn=with_session,
    )
    assert len(found) == 1
    assert found[0].paper_id == result.paper_id

    # Clean up
    _cleanup_paper_and_bib(migrated_db_url, [result.paper_id])


# ---------------------------------------------------------------------------
# update_field
# ---------------------------------------------------------------------------


def test_update_field_title(migrated_db_url: str) -> None:
    """update_field updates the title of an existing paper."""
    data = PaperCreate(
        title="Before Update",
        authors=["Updater, A."],
        bibtex_key="ServiceUpdate2026",
        summary="Before.",
        bibtex_text="@article{ServiceUpdate2026}",
    )
    result = paper_service.add_paper(
        data, database_url=migrated_db_url, with_session_fn=with_session
    )
    paper_service.update_field(
        result.paper_id,
        "title",
        "After Update",
        database_url=migrated_db_url,
        with_session_fn=with_session,
    )
    found = paper_service.search_by_title(
        "After Update", database_url=migrated_db_url, with_session_fn=with_session
    )
    assert len(found) == 1

    # Clean up
    _cleanup_paper_and_bib(migrated_db_url, [result.paper_id])


# ---------------------------------------------------------------------------
# delete_paper
# ---------------------------------------------------------------------------


def test_delete_paper(migrated_db_url: str) -> None:
    """delete_paper removes the paper from the DB."""
    data = PaperCreate(
        title="To Be Deleted by Service",
        authors=["Deleter, A."],
        bibtex_key="ServiceDelete2026",
        summary="Will be deleted.",
        bibtex_text="@article{ServiceDelete2026}",
    )
    result = paper_service.add_paper(
        data, database_url=migrated_db_url, with_session_fn=with_session
    )
    paper_service.delete_paper(
        result.paper_id, database_url=migrated_db_url, with_session_fn=with_session
    )
    found = paper_service.search_by_title(
        "To Be Deleted by Service",
        database_url=migrated_db_url,
        with_session_fn=with_session,
    )
    assert found == []


def test_delete_paper_not_found(migrated_db_url: str) -> None:
    """delete_paper raises ValueError for a non-existent paper id."""
    with pytest.raises(ValueError):
        paper_service.delete_paper(
            999999, database_url=migrated_db_url, with_session_fn=with_session
        )
