"""Integration tests for PaperRepository, AuthorRepository, BibRepository.

These tests run against the real ephemeral PostgreSQL database provisioned by
:func:`tests.conftest.db_engine`.  Mocking the SQLAlchemy session or
repositories is forbidden per constitution Principle II.

Test coverage:
- search_by_title: found, not found
- search_by_author: found, not found
- add_paper: success, duplicate bibtex_id raises ValueError
- delete_paper: success, not found raises ValueError
- BibRepository.update_bibtex: success, duplicate value raises ValueError
- AuthorRepository.update_author_name: success
"""

from __future__ import annotations

import pytest

from paper_sorts.db.repositories import (
    AuthorRepository,
    BibRepository,
    PaperCreate,
    PaperRepository,
)
from paper_sorts.db.session import with_session


class TestSearchByTitle:
    """Tests for PaperRepository.search_by_title."""

    def test_found_single(self, db_engine: object) -> None:
        """Search for a title that exists exactly once returns one result."""
        with with_session(db_engine) as session:  # type: ignore[arg-type]
            results = PaperRepository.search_by_title(
                session,
                "Large-scale Self- an Semi-Supervised learning for speech translation",
            )
        assert len(results) == 1
        result = results[0]
        assert result.bibtex_id == "Wang2021LargeScaleSA"
        assert "Pino, J." in result.authors

    def test_found_multiple_duplicate_titles(self, db_engine: object) -> None:
        """Search for a title shared by two papers returns both."""
        with with_session(db_engine) as session:  # type: ignore[arg-type]
            results = PaperRepository.search_by_title(session, "Duplicate Title Paper")
        assert len(results) == 2
        keys = {r.bibtex_id for r in results}
        assert keys == {"Dup2021A", "Dup2021B"}

    def test_not_found(self, db_engine: object) -> None:
        """Search for a non-existent title returns empty list."""
        with with_session(db_engine) as session:  # type: ignore[arg-type]
            results = PaperRepository.search_by_title(session, "no such title")
        assert results == []

    def test_multi_author_join(self, db_engine: object) -> None:
        """Multi-author paper returns all authors joined correctly."""
        with with_session(db_engine) as session:  # type: ignore[arg-type]
            results = PaperRepository.search_by_title(
                session, "Direct speech-to-speech translation with discrete units"
            )
        assert len(results) == 1
        authors = results[0].authors
        assert "Lee, Ann" in authors
        assert "Pino, J." in authors
        assert "Hsu, Wei-Ning" in authors


class TestSearchByAuthor:
    """Tests for PaperRepository.search_by_author."""

    def test_found(self, db_engine: object) -> None:
        """Searching by a known author returns that author's papers."""
        with with_session(db_engine) as session:  # type: ignore[arg-type]
            results = PaperRepository.search_by_author(session, "Pino, J.")
        assert len(results) >= 1
        titles = [r.title for r in results]
        assert "Large-scale Self- an Semi-Supervised learning for speech translation" in titles

    def test_not_found(self, db_engine: object) -> None:
        """Searching by an unknown author returns empty list."""
        with with_session(db_engine) as session:  # type: ignore[arg-type]
            results = PaperRepository.search_by_author(session, "no author")
        assert results == []


class TestAddPaper:
    """Tests for PaperRepository.add_paper."""

    def test_add_success(self, db_engine: object) -> None:
        """Adding a new paper succeeds and the paper is retrievable."""
        paper = PaperCreate(
            title="Test Add Paper",
            contents="Test summary.",
            bibtex_id="TestAdd2026",
            bibtex="@article{TestAdd2026, author={Test, A.}, title={Test Add Paper}}",
            authors=["Test, A."],
        )
        with with_session(db_engine) as session:  # type: ignore[arg-type]
            PaperRepository.add_paper(session, paper)

        with with_session(db_engine) as session:  # type: ignore[arg-type]
            results = PaperRepository.search_by_title(session, "Test Add Paper")
        assert len(results) == 1
        assert results[0].bibtex_id == "TestAdd2026"

    def test_duplicate_bibtex_id_raises(self, db_engine: object) -> None:
        """Adding a paper with a duplicate bibtex_id raises ValueError."""
        paper = PaperCreate(
            title="Unique Title For Dup Test",
            contents="summary",
            bibtex_id="Wang2021LargeScaleSA",  # already in seed data
            bibtex="@article{Wang2021LargeScaleSA, author={X}, title={X}}",
            authors=["X, Y"],
        )
        with pytest.raises(ValueError, match="already exists"):
            with with_session(db_engine) as session:  # type: ignore[arg-type]
                PaperRepository.add_paper(session, paper)

    def test_empty_authors_raises(self, db_engine: object) -> None:
        """Adding a paper with no authors raises ValueError."""
        paper = PaperCreate(
            title="No Author Paper",
            contents="summary",
            bibtex_id="NoAuth2026",
            bibtex="@article{NoAuth2026}",
            authors=[],
        )
        with pytest.raises(ValueError, match="at least one author"):
            with with_session(db_engine) as session:  # type: ignore[arg-type]
                PaperRepository.add_paper(session, paper)


class TestDeletePaper:
    """Tests for PaperRepository.delete_paper."""

    def test_delete_success(self, db_engine: object) -> None:
        """Deleting an existing paper removes it from search results."""
        paper = PaperCreate(
            title="To Be Deleted",
            contents="summary",
            bibtex_id="Delete2026",
            bibtex="@article{Delete2026, author={D.}, title={To Be Deleted}}",
            authors=["D., Author"],
        )
        with with_session(db_engine) as session:  # type: ignore[arg-type]
            PaperRepository.add_paper(session, paper)

        with with_session(db_engine) as session:  # type: ignore[arg-type]
            PaperRepository.delete_paper(session, "Delete2026")

        with with_session(db_engine) as session:  # type: ignore[arg-type]
            results = PaperRepository.search_by_title(session, "To Be Deleted")
        assert results == []

    def test_delete_not_found_raises(self, db_engine: object) -> None:
        """Deleting a non-existent paper raises ValueError."""
        with pytest.raises(ValueError, match="not found"):
            with with_session(db_engine) as session:  # type: ignore[arg-type]
                PaperRepository.delete_paper(session, "NonExistentKey9999")


class TestBibRepository:
    """Tests for BibRepository."""

    def test_get_bibtex_found(self, db_engine: object) -> None:
        """Getting BibTeX for a known key returns the BibTeX string."""
        with with_session(db_engine) as session:  # type: ignore[arg-type]
            bibtex = BibRepository.get_bibtex(session, "Wang2021LargeScaleSA")
        assert bibtex is not None
        assert "Wang2021LargeScaleSA" in bibtex

    def test_get_bibtex_not_found(self, db_engine: object) -> None:
        """Getting BibTeX for an unknown key returns None."""
        with with_session(db_engine) as session:  # type: ignore[arg-type]
            bibtex = BibRepository.get_bibtex(session, "DoesNotExist")
        assert bibtex is None

    def test_update_bibtex_success(self, db_engine: object) -> None:
        """Updating BibTeX for a known key persists the new value."""
        new_bibtex = "@article{Accent2021, author={New}, title={Updated}}"
        with with_session(db_engine) as session:  # type: ignore[arg-type]
            BibRepository.update_bibtex(session, "Accent2021", new_bibtex)

        with with_session(db_engine) as session:  # type: ignore[arg-type]
            bibtex = BibRepository.get_bibtex(session, "Accent2021")
        assert bibtex == new_bibtex

    def test_update_bibtex_duplicate_raises(self, db_engine: object) -> None:
        """Updating BibTeX to an existing value raises ValueError (UNIQUE)."""
        # Get the current BibTeX of Lee2021DirectSpeech to use as a duplicate
        with with_session(db_engine) as session:  # type: ignore[arg-type]
            existing = BibRepository.get_bibtex(session, "Lee2021DirectSpeech")
        assert existing is not None

        with pytest.raises(ValueError, match="unique|already exists"):
            with with_session(db_engine) as session:  # type: ignore[arg-type]
                BibRepository.update_bibtex(session, "Wang2021LargeScaleSA", existing)


class TestAuthorRepository:
    """Tests for AuthorRepository.update_author_name."""

    def test_update_author_name_renames(self, db_engine: object) -> None:
        """Renaming an author to a new name updates the author record."""
        paper = PaperCreate(
            title="Author Rename Test",
            contents="summary",
            bibtex_id="AuthRen2026",
            bibtex="@article{AuthRen2026, author={Old, Author}}",
            authors=["Old, Author"],
        )
        with with_session(db_engine) as session:  # type: ignore[arg-type]
            PaperRepository.add_paper(session, paper)

        with with_session(db_engine) as session:  # type: ignore[arg-type]
            AuthorRepository.update_author_name(session, "Old, Author", "New, Author")

        with with_session(db_engine) as session:  # type: ignore[arg-type]
            results = PaperRepository.search_by_author(session, "New, Author")
        assert any(r.bibtex_id == "AuthRen2026" for r in results)

    def test_update_author_not_found_raises(self, db_engine: object) -> None:
        """Renaming a non-existent author raises ValueError."""
        with pytest.raises(ValueError, match="not found"):
            with with_session(db_engine) as session:  # type: ignore[arg-type]
                AuthorRepository.update_author_name(session, "Ghost, Author", "Real, Author")
