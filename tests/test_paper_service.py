"""Service-layer integration tests for paper_sorts.

Tests call paper_service functions (which open their own sessions internally)
against the same ephemeral PostgreSQL instance used by test_repositories.

Seed data: tests/fixtures/seed_papers.SEED_PAPERS
"""

import pytest

from paper_sorts.db.repositories import PaperCreate
from paper_sorts.services import paper_service
from tests.fixtures.seed_papers import SEED_PAPERS


@pytest.fixture(autouse=True)
def seed(db_engine: object) -> None:  # type: ignore[type-arg]
    """Seed the database before each test.

    :param db_engine: Session-scoped engine fixture.
    """
    from sqlalchemy.engine import Engine

    from paper_sorts.db.repositories import PaperRepository
    from paper_sorts.db.session import with_session

    assert isinstance(db_engine, Engine)

    with with_session(db_engine) as session:
        repo = PaperRepository(session)
        for paper in SEED_PAPERS:
            try:
                repo.add(paper)
            except ValueError:
                pass


@pytest.fixture(autouse=True)
def cleanup(db_engine: object) -> object:  # type: ignore[type-arg, return]
    """Roll back seed data after each service test.

    Uses a separate session to delete all rows from all tables.
    """
    from sqlalchemy import text
    from sqlalchemy.engine import Engine

    assert isinstance(db_engine, Engine)
    yield
    from paper_sorts.db.session import with_session

    with with_session(db_engine) as session:
        session.execute(text("DELETE FROM authors_papers"))
        session.execute(text("DELETE FROM papers"))
        session.execute(text("DELETE FROM bib"))
        session.execute(text("DELETE FROM authors_id"))


class TestSearchByTitle:
    """Service-layer search_by_title tests."""

    def test_found(self, db_engine: object) -> None:  # type: ignore[type-arg]
        """search_by_title returns matching papers via the service layer."""
        from sqlalchemy.engine import Engine

        assert isinstance(db_engine, Engine)
        results = paper_service.search_by_title(db_engine, "Survey of Low-Resource NMT")
        assert len(results) == 1
        assert results[0].bibtex_id == "survey2023"

    def test_not_found(self, db_engine: object) -> None:  # type: ignore[type-arg]
        """search_by_title returns empty list when title is absent."""
        from sqlalchemy.engine import Engine

        assert isinstance(db_engine, Engine)
        results = paper_service.search_by_title(db_engine, "Nonexistent Title")
        assert results == []


class TestSearchByAuthor:
    """Service-layer search_by_author tests."""

    def test_found(self, db_engine: object) -> None:  # type: ignore[type-arg]
        """search_by_author finds papers via the service layer."""
        from sqlalchemy.engine import Engine

        assert isinstance(db_engine, Engine)
        results = paper_service.search_by_author(db_engine, "Smith, Alice")
        assert len(results) == 1

    def test_not_found(self, db_engine: object) -> None:  # type: ignore[type-arg]
        """search_by_author returns empty list for unknown author."""
        from sqlalchemy.engine import Engine

        assert isinstance(db_engine, Engine)
        results = paper_service.search_by_author(db_engine, "Nobody, Unknown")
        assert results == []


class TestAddPaper:
    """Service-layer add_paper tests."""

    def test_add_and_retrieve(self, db_engine: object) -> None:  # type: ignore[type-arg]
        """add_paper persists a paper retrievable by title."""
        from sqlalchemy.engine import Engine

        assert isinstance(db_engine, Engine)
        new_paper = PaperCreate(
            title="Service Layer Test Paper",
            contents="Test summary.",
            bibtex_id="service_test_001",
            authors=["Tester, Unit"],
            bibtex="@article{service_test_001}",
        )
        paper_service.add_paper(db_engine, new_paper)
        results = paper_service.search_by_title(db_engine, "Service Layer Test Paper")
        assert len(results) == 1

    def test_duplicate_raises(self, db_engine: object) -> None:  # type: ignore[type-arg]
        """add_paper raises ValueError on duplicate bibtex_id."""
        from sqlalchemy.engine import Engine

        assert isinstance(db_engine, Engine)
        with pytest.raises(ValueError):
            paper_service.add_paper(db_engine, SEED_PAPERS[0])


class TestDeletePaper:
    """Service-layer delete_paper tests."""

    def test_delete_removes_paper(self, db_engine: object) -> None:  # type: ignore[type-arg]
        """delete_paper removes the paper from search results."""
        from sqlalchemy.engine import Engine

        assert isinstance(db_engine, Engine)
        results = paper_service.search_by_title(db_engine, "Elastic Weight Consolidation for NMT")
        assert results
        paper_service.delete_paper(db_engine, results[0].id)
        after = paper_service.search_by_title(db_engine, "Elastic Weight Consolidation for NMT")
        assert after == []
