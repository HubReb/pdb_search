"""Integration tests for the paper_sorts persistence layer.

Tests run against a real ephemeral PostgreSQL instance (no mocking of
SQLAlchemy session — constitution Principle II).

Seed data is co-located in tests/fixtures/seed_papers.py.
"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import IntegrityError

from paper_sorts.db.repositories import PaperCreate, PaperRepository
from paper_sorts.db.session import with_session
from tests.fixtures.seed_papers import SEED_PAPERS


def test_add_and_find_by_title(db_url: str) -> None:
    """A paper added to the DB is retrievable by exact title match."""
    paper = SEED_PAPERS[2]  # Wang2021LargeScaleSA
    with with_session(db_url) as session:
        repo = PaperRepository(session)
        added = repo.add(paper)
        results = repo.search_by_title("Large-Scale")
    assert len(results) == 1
    assert results[0].bibtex_id == "Wang2021LargeScaleSA"
    assert results[0].id == added.id


def test_add_and_find_by_author(db_url: str) -> None:
    """A paper added to the DB is retrievable by author name substring."""
    paper = SEED_PAPERS[4]  # He2016DeepRL — He, Kaiming; Zhang, Xiangyu
    with with_session(db_url) as session:
        repo = PaperRepository(session)
        repo.add(paper)
        results = repo.search_by_author("Kaiming")
    assert len(results) == 1
    assert results[0].bibtex_id == "He2016DeepRL"
    assert "He, Kaiming" in results[0].authors


def test_search_returns_multiple_matches(db_url: str) -> None:
    """Searching for a shared title substring returns all matching papers."""
    # SEED_PAPERS[0] and [1] share "Attention Is All You Need" as title
    with with_session(db_url) as session:
        repo = PaperRepository(session)
        for paper in SEED_PAPERS[:2]:
            repo.add(paper)
        results = repo.search_by_title("Attention Is All You Need")
    assert len(results) == 2


def test_update_title(db_url: str) -> None:
    """Updating a paper's title persists the new value."""
    paper = SEED_PAPERS[2]
    with with_session(db_url) as session:
        repo = PaperRepository(session)
        added = repo.add(paper)
        updated = repo.update_title(added.id, "New Title")
    assert updated.title == "New Title"
    # Verify persisted
    with with_session(db_url) as session:
        repo = PaperRepository(session)
        fetched = repo.get_by_id(added.id)
    assert fetched is not None
    assert fetched.title == "New Title"


def test_delete_paper(db_url: str) -> None:
    """Deleting a paper removes it and its authorship links."""
    paper = SEED_PAPERS[2]
    with with_session(db_url) as session:
        repo = PaperRepository(session)
        added = repo.add(paper)
        paper_id = added.id

    with with_session(db_url) as session:
        repo = PaperRepository(session)
        repo.delete(paper_id)

    with with_session(db_url) as session:
        repo = PaperRepository(session)
        fetched = repo.get_by_id(paper_id)
    assert fetched is None


def test_duplicate_bibtex_id_rejected(db_url: str) -> None:
    """Adding two papers with the same bibtex_id raises IntegrityError."""
    paper = SEED_PAPERS[2]
    with with_session(db_url) as session:
        repo = PaperRepository(session)
        repo.add(paper)
    with pytest.raises(IntegrityError), with_session(db_url) as session:
        repo = PaperRepository(session)
        repo.add(paper)


def test_bibtex_unique_constraint(db_url: str) -> None:
    """Adding two entries with identical BibTeX text raises IntegrityError."""
    paper1 = SEED_PAPERS[2]
    paper2 = PaperCreate(
        title="Different Title",
        contents="Different contents",
        bibtex_id="DifferentKey2024",
        bibtex=paper1.bibtex,  # same bibtex text → UNIQUE violation
        authors=["Smith, John"],
    )
    with with_session(db_url) as session:
        repo = PaperRepository(session)
        repo.add(paper1)
    with pytest.raises(IntegrityError), with_session(db_url) as session:
        repo = PaperRepository(session)
        repo.add(paper2)


def test_get_by_id_not_found(db_url: str) -> None:
    """get_by_id returns None for a non-existent paper id."""
    with with_session(db_url) as session:
        repo = PaperRepository(session)
        result = repo.get_by_id(99999)
    assert result is None


def test_update_authors(db_url: str) -> None:
    """Updating authors replaces the author list."""
    paper = SEED_PAPERS[4]  # He, Kaiming; Zhang, Xiangyu
    with with_session(db_url) as session:
        repo = PaperRepository(session)
        added = repo.add(paper)
        updated = repo.update_authors(added.id, ["Smith, John"])
    assert updated.authors == ["Smith, John"]


def test_search_by_author_no_results(db_url: str) -> None:
    """Searching for a non-existent author returns empty list."""
    with with_session(db_url) as session:
        repo = PaperRepository(session)
        results = repo.search_by_author("Zzznonexistent")
    assert results == []
