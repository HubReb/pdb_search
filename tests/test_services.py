"""Integration tests for the paper_sorts service layer.

Tests run against the ephemeral PostgreSQL instance.
No mocking of the SQLAlchemy session or repositories (constitution Principle II).
"""

from __future__ import annotations

import pytest
from sqlalchemy.orm import Session

from paper_sorts.db.repositories import PaperCreate
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service
from tests.fixtures.seed_papers import PAPER_1, PAPER_2


class TestSearchServices:
    """Tests for search service functions."""

    def test_search_by_title_returns_paper(self, db_session: Session) -> None:
        """search_by_title returns the seeded PAPER_1 by exact title."""
        results = paper_service.search_by_title(db_session, PAPER_1.title)
        assert len(results) == 1
        assert results[0].title == PAPER_1.title

    def test_search_by_title_empty_for_unknown(self, db_session: Session) -> None:
        """search_by_title returns empty list for unknown title."""
        results = paper_service.search_by_title(db_session, "Absolutely Unknown Title ZZZZ")
        assert results == []

    def test_search_by_author_returns_papers(self, db_session: Session) -> None:
        """search_by_author finds PAPER_2 via 'Wang, Changhan'."""
        results = paper_service.search_by_author(db_session, "Wang, Changhan")
        assert len(results) >= 1
        assert any(r.title == PAPER_2.title for r in results)

    def test_search_by_author_empty_for_unknown(self, db_session: Session) -> None:
        """search_by_author returns empty list for unknown author."""
        results = paper_service.search_by_author(db_session, "Unknown, Nobody9999")
        assert results == []


class TestAddService:
    """Tests for add_paper service function."""

    def test_add_paper_success(self, ephemeral_db_url: str) -> None:
        """add_paper returns True and the paper is retrievable."""
        new_paper = PaperCreate(
            title="Service Add Test",
            contents="test summary",
            bibtex_id="ServiceAddTest2026",
            bibtex="@misc{ServiceAddTest2026}",
            authors=["Service, Tester"],
        )
        with with_session(ephemeral_db_url) as session:
            result = paper_service.add_paper(session, new_paper)
        assert result is True

        with with_session(ephemeral_db_url) as session:
            found = paper_service.search_by_title(session, "Service Add Test")
            assert len(found) == 1

        # Cleanup
        with with_session(ephemeral_db_url) as session:
            paper_service.delete_paper(session, "ServiceAddTest2026")

    def test_add_duplicate_returns_false(self, ephemeral_db_url: str) -> None:
        """add_paper returns False when bibtex_id already exists."""
        with with_session(ephemeral_db_url) as session:
            result = paper_service.add_paper(session, PAPER_1)
        assert result is False


class TestUpdateService:
    """Tests for update_field service function."""

    def test_update_title(self, ephemeral_db_url: str) -> None:
        """update_field updates a paper's title."""
        paper = PaperCreate(
            title="Service Update Title Before",
            contents="x",
            bibtex_id="SvcUpdateTitle2026",
            bibtex="@misc{SvcUpdateTitle2026}",
            authors=["Svc, T"],
        )
        with with_session(ephemeral_db_url) as session:
            paper_service.add_paper(session, paper)

        with with_session(ephemeral_db_url) as session:
            paper_service.update_field(
                session, "papers", "title", "Service Update Title Before", "Title After"
            )

        with with_session(ephemeral_db_url) as session:
            results = paper_service.search_by_title(session, "Title After")
            assert len(results) == 1

        # Cleanup
        with with_session(ephemeral_db_url) as session:
            paper_service.delete_paper(session, "SvcUpdateTitle2026")

    def test_update_contents(self, ephemeral_db_url: str) -> None:
        """update_field updates a paper's summary (contents)."""
        paper = PaperCreate(
            title="Service Update Contents Test",
            contents="old",
            bibtex_id="SvcUpdateContents2026",
            bibtex="@misc{SvcUpdateContents2026}",
            authors=["Svc, C"],
        )
        with with_session(ephemeral_db_url) as session:
            paper_service.add_paper(session, paper)

        with with_session(ephemeral_db_url) as session:
            paper_service.update_field(
                session, "papers", "contents", "Service Update Contents Test", "new summary"
            )

        with with_session(ephemeral_db_url) as session:
            results = paper_service.search_by_title(session, "Service Update Contents Test")
            assert results[0].contents == "new summary"

        # Cleanup
        with with_session(ephemeral_db_url) as session:
            paper_service.delete_paper(session, "SvcUpdateContents2026")

    def test_update_bibtex(self, ephemeral_db_url: str) -> None:
        """update_field updates a bib entry's bibtex text."""
        paper = PaperCreate(
            title="Service Update Bib Test",
            contents="x",
            bibtex_id="SvcUpdateBib2026",
            bibtex="@misc{SvcUpdateBib2026, title={old}}",
            authors=["Svc, B"],
        )
        with with_session(ephemeral_db_url) as session:
            paper_service.add_paper(session, paper)

        with with_session(ephemeral_db_url) as session:
            paper_service.update_field(
                session, "bib", "bibtex", "SvcUpdateBib2026", "@misc{SvcUpdateBib2026, title={new}}"
            )

        with with_session(ephemeral_db_url) as session:
            results = paper_service.search_by_title(session, "Service Update Bib Test")
            assert "new" in results[0].bibtex

        # Cleanup
        with with_session(ephemeral_db_url) as session:
            paper_service.delete_paper(session, "SvcUpdateBib2026")

    def test_update_author_name(self, ephemeral_db_url: str) -> None:
        """update_field renames an author."""
        paper = PaperCreate(
            title="Service Update Author Test",
            contents="x",
            bibtex_id="SvcUpdateAuthor2026",
            bibtex="@misc{SvcUpdateAuthor2026}",
            authors=["OldAuthor, Svc"],
        )
        with with_session(ephemeral_db_url) as session:
            paper_service.add_paper(session, paper)

        with with_session(ephemeral_db_url) as session:
            paper_service.update_field(
                session, "authors_id", "author", "OldAuthor, Svc", "NewAuthor, Svc"
            )

        with with_session(ephemeral_db_url) as session:
            results = paper_service.search_by_author(session, "NewAuthor, Svc")
            assert len(results) >= 1

        # Cleanup
        with with_session(ephemeral_db_url) as session:
            paper_service.delete_paper(session, "SvcUpdateAuthor2026")

    def test_update_invalid_column_raises(self, ephemeral_db_url: str) -> None:
        """update_field raises ValueError for invalid column."""
        with with_session(ephemeral_db_url) as session, pytest.raises(ValueError):
            paper_service.update_field(session, "papers", "invalid_col", "x", "y")

    def test_update_bib_invalid_column_raises(self, ephemeral_db_url: str) -> None:
        """update_field raises ValueError for invalid bib column."""
        with with_session(ephemeral_db_url) as session, pytest.raises(ValueError):
            paper_service.update_field(session, "bib", "invalid", "x", "y")


class TestDeleteService:
    """Tests for delete_paper service function."""

    def test_delete_existing(self, ephemeral_db_url: str) -> None:
        """delete_paper returns True for an existing paper."""
        paper = PaperCreate(
            title="Service Delete Test",
            contents="x",
            bibtex_id="SvcDeleteTest2026",
            bibtex="@misc{SvcDeleteTest2026}",
            authors=["Svc, Del"],
        )
        with with_session(ephemeral_db_url) as session:
            paper_service.add_paper(session, paper)

        with with_session(ephemeral_db_url) as session:
            result = paper_service.delete_paper(session, "SvcDeleteTest2026")
        assert result is True

    def test_delete_nonexistent_returns_false(self, ephemeral_db_url: str) -> None:
        """delete_paper returns False for a non-existent paper."""
        with with_session(ephemeral_db_url) as session:
            result = paper_service.delete_paper(session, "DoesNotExist9999")
        assert result is False
