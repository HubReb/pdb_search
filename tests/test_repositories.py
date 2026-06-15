"""Integration tests for the paper_sorts persistence layer.

All tests run against the ephemeral PostgreSQL instance provided by conftest.py.
No mocking of SQLAlchemy session, repositories, or the database driver
(constitution Principle II).

Seed data is from tests/fixtures/seed_papers.py — assertions reference SEED_PAPERS
so the relationship is visible at review time.
"""

from __future__ import annotations

import pytest
from sqlalchemy.orm import Session

from paper_sorts.db.repositories import (
    AuthorRepository,
    BibRepository,
    PaperCreate,
    PaperRepository,
)
from paper_sorts.db.session import with_session
from tests.fixtures.seed_papers import PAPER_1, PAPER_2


class TestPaperRepository:
    """Tests for PaperRepository CRUD and search operations."""

    def test_search_by_title_found(self, db_session: Session) -> None:
        """Search by exact title of PAPER_1 returns one result."""
        repo = PaperRepository(db_session)
        results = repo.search_by_title(PAPER_1.title)
        assert len(results) == 1
        assert results[0].title == PAPER_1.title
        assert results[0].bibtex_id == PAPER_1.bibtex_id

    def test_search_by_title_not_found(self, db_session: Session) -> None:
        """Search by non-existent title returns empty list."""
        repo = PaperRepository(db_session)
        results = repo.search_by_title("Totally Non-Existent Paper Title ZZZZZ")
        assert results == []

    def test_search_by_title_includes_authors(self, db_session: Session) -> None:
        """Search result for PAPER_2 includes all three authors."""
        repo = PaperRepository(db_session)
        results = repo.search_by_title(PAPER_2.title)
        assert len(results) == 1
        for author in PAPER_2.authors:
            assert author in results[0].authors

    def test_search_by_author_found(self, db_session: Session) -> None:
        """Search by 'Pino, J.' (PAPER_2 co-author) returns at least one paper."""
        repo = PaperRepository(db_session)
        results = repo.search_by_author("Pino, J.")
        assert len(results) >= 1
        titles = [r.title for r in results]
        assert PAPER_2.title in titles

    def test_search_by_author_not_found(self, db_session: Session) -> None:
        """Search by non-existent author returns empty list."""
        repo = PaperRepository(db_session)
        results = repo.search_by_author("NoSuch, Author")
        assert results == []

    def test_add_and_delete(self, ephemeral_db_url: str) -> None:
        """Add a new paper, verify it's retrievable, then delete it."""
        new_paper = PaperCreate(
            title="Test Paper for Add Delete",
            contents="Test contents.",
            bibtex_id="TestAdd2026",
            bibtex="@misc{TestAdd2026, title={Test}, year={2026}}",
            authors=["Test, Author"],
        )
        with with_session(ephemeral_db_url) as session:
            repo = PaperRepository(session)
            repo.add(new_paper)

        # Verify retrievable
        with with_session(ephemeral_db_url) as session:
            repo = PaperRepository(session)
            results = repo.search_by_title("Test Paper for Add Delete")
            assert len(results) == 1
            assert results[0].bibtex_id == "TestAdd2026"

        # Delete
        with with_session(ephemeral_db_url) as session:
            repo = PaperRepository(session)
            deleted = repo.delete("TestAdd2026")
            assert deleted is True

        # Verify gone
        with with_session(ephemeral_db_url) as session:
            repo = PaperRepository(session)
            results = repo.search_by_title("Test Paper for Add Delete")
            assert results == []

    def test_add_duplicate_bibtex_id_raises(self, ephemeral_db_url: str) -> None:
        """Adding a paper with an existing bibtex_id raises ValueError."""
        with with_session(ephemeral_db_url) as session:
            repo = PaperRepository(session)
            with pytest.raises(ValueError, match="already exists"):
                repo.add(PAPER_1)  # PAPER_1 is already seeded

    def test_update_title(self, ephemeral_db_url: str) -> None:
        """Update a paper's title and verify the change persists."""
        # Add a disposable paper
        paper = PaperCreate(
            title="Title Before Update",
            contents="contents",
            bibtex_id="TitleUpdateTest2026",
            bibtex="@misc{TitleUpdateTest2026}",
            authors=["Test, User"],
        )
        with with_session(ephemeral_db_url) as session:
            repo = PaperRepository(session)
            added = repo.add(paper)
            paper_id = added.id

        with with_session(ephemeral_db_url) as session:
            repo = PaperRepository(session)
            repo.update_title(paper_id, "Title After Update")

        with with_session(ephemeral_db_url) as session:
            repo = PaperRepository(session)
            results = repo.search_by_title("Title After Update")
            assert len(results) == 1

        # Cleanup
        with with_session(ephemeral_db_url) as session:
            repo = PaperRepository(session)
            repo.delete("TitleUpdateTest2026")

    def test_update_contents(self, ephemeral_db_url: str) -> None:
        """Update a paper's contents (summary) and verify."""
        paper = PaperCreate(
            title="Contents Update Test",
            contents="old contents",
            bibtex_id="ContentsUpdateTest2026",
            bibtex="@misc{ContentsUpdateTest2026}",
            authors=["Test, C"],
        )
        with with_session(ephemeral_db_url) as session:
            repo = PaperRepository(session)
            added = repo.add(paper)
            paper_id = added.id

        with with_session(ephemeral_db_url) as session:
            repo = PaperRepository(session)
            repo.update_contents(paper_id, "new contents")

        with with_session(ephemeral_db_url) as session:
            repo = PaperRepository(session)
            results = repo.search_by_title("Contents Update Test")
            assert results[0].contents == "new contents"

        # Cleanup
        with with_session(ephemeral_db_url) as session:
            repo = PaperRepository(session)
            repo.delete("ContentsUpdateTest2026")

    def test_delete_nonexistent_returns_false(self, ephemeral_db_url: str) -> None:
        """Deleting a non-existent bibtex_id returns False (not an error)."""
        with with_session(ephemeral_db_url) as session:
            repo = PaperRepository(session)
            result = repo.delete("NonExistentKey9999")
            assert result is False


class TestBibRepository:
    """Tests for BibRepository operations."""

    def test_find_by_id_found(self, db_session: Session) -> None:
        """Find an existing bib entry by key."""
        repo = BibRepository(db_session)
        bib = repo.find_by_id(PAPER_1.bibtex_id)
        assert bib is not None
        assert bib.bibtex_id == PAPER_1.bibtex_id

    def test_find_by_id_not_found(self, db_session: Session) -> None:
        """Find returns None for non-existent key."""
        repo = BibRepository(db_session)
        bib = repo.find_by_id("NonExistentKey999")
        assert bib is None

    def test_update_bibtex(self, ephemeral_db_url: str) -> None:
        """Update the bibtex text for an existing entry."""
        paper = PaperCreate(
            title="Bib Update Test",
            contents="contents",
            bibtex_id="BibUpdateTest2026",
            bibtex="@misc{BibUpdateTest2026, title={old}}",
            authors=["Test, B"],
        )
        with with_session(ephemeral_db_url) as session:
            repo = PaperRepository(session)
            repo.add(paper)

        with with_session(ephemeral_db_url) as session:
            bib_repo = BibRepository(session)
            bib_repo.update("BibUpdateTest2026", "@misc{BibUpdateTest2026, title={new}}")

        with with_session(ephemeral_db_url) as session:
            bib_repo = BibRepository(session)
            bib = bib_repo.find_by_id("BibUpdateTest2026")
            assert bib is not None
            assert "new" in bib.bibtex

        # Cleanup
        with with_session(ephemeral_db_url) as session:
            repo = PaperRepository(session)
            repo.delete("BibUpdateTest2026")


class TestAuthorRepository:
    """Tests for AuthorRepository operations."""

    def test_find_or_create_creates_new(self, ephemeral_db_url: str) -> None:
        """find_or_create inserts a new author if not present."""
        author_name = "NewAuthor, Test2026Unique"
        with with_session(ephemeral_db_url) as session:
            repo = AuthorRepository(session)
            author = repo.find_or_create(author_name)
            assert author.id is not None
            assert author.author == author_name

    def test_find_or_create_finds_existing(self, db_session: Session) -> None:
        """find_or_create returns existing author without duplicate insertion."""
        repo = AuthorRepository(db_session)
        # PAPER_1.authors[0] = "Lee, Ann" is in seed data
        a1 = repo.find_or_create(PAPER_1.authors[0])
        a2 = repo.find_or_create(PAPER_1.authors[0])
        assert a1.id == a2.id

    def test_update_name(self, ephemeral_db_url: str) -> None:
        """Rename an author and verify the new name persists."""
        paper = PaperCreate(
            title="Author Rename Test Paper",
            contents="test",
            bibtex_id="AuthorRenameTest2026",
            bibtex="@misc{AuthorRenameTest2026}",
            authors=["OldName, Test"],
        )
        with with_session(ephemeral_db_url) as session:
            repo = PaperRepository(session)
            repo.add(paper)

        with with_session(ephemeral_db_url) as session:
            a_repo = AuthorRepository(session)
            a_repo.update_name("OldName, Test", "NewName, Test")

        with with_session(ephemeral_db_url) as session:
            p_repo = PaperRepository(session)
            results = p_repo.search_by_author("NewName, Test")
            assert len(results) >= 1

        # Cleanup
        with with_session(ephemeral_db_url) as session:
            p_repo = PaperRepository(session)
            p_repo.delete("AuthorRenameTest2026")

    def test_update_name_nonexistent_raises(self, ephemeral_db_url: str) -> None:
        """Renaming a non-existent author raises ValueError."""
        with with_session(ephemeral_db_url) as session:
            a_repo = AuthorRepository(session)
            with pytest.raises(ValueError, match="not found"):
                a_repo.update_name("NonExistent, Author9999", "Other")
