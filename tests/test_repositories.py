"""Integration tests for db/repositories.py.

All tests run against the ephemeral PostgreSQL provisioned by pytest-postgresql.
No mocking of the SQLAlchemy session or database driver.
"""

import pytest
from sqlalchemy.orm import Session

from paper_sorts.db.repositories import (
    AuthorRepository,
    BibRepository,
    PaperCreate,
    PaperRepository,
)


class TestBibRepository:
    """Tests for BibRepository."""

    def test_add_and_get_by_id(self, db_session: Session) -> None:
        """Adding a bib entry and retrieving it by key round-trips correctly."""
        repo = BibRepository(db_session)
        repo.add("TestKey2024", "@article{TestKey2024, title={Test}}")
        db_session.flush()
        retrieved = repo.get_by_id("TestKey2024")
        assert retrieved is not None
        assert retrieved.bibtex_id == "TestKey2024"

    def test_get_by_id_missing(self, db_session: Session) -> None:
        """get_by_id returns None when the key doesn't exist."""
        repo = BibRepository(db_session)
        assert repo.get_by_id("NonExistentKey") is None

    def test_exists_true(self, db_session: Session) -> None:
        """exists() returns True after adding an entry."""
        repo = BibRepository(db_session)
        repo.add("ExKey2024", "@article{ExKey2024}")
        db_session.flush()
        assert repo.exists("ExKey2024") is True

    def test_exists_false(self, db_session: Session) -> None:
        """exists() returns False for a key not yet added."""
        repo = BibRepository(db_session)
        assert repo.exists("NotThere") is False


class TestAuthorRepository:
    """Tests for AuthorRepository."""

    def test_get_or_create_creates_new(self, db_session: Session) -> None:
        """get_or_create inserts a new author if not present."""
        repo = AuthorRepository(db_session)
        author = repo.get_or_create("Smith, John")
        db_session.flush()
        assert author.author == "Smith, John"
        assert author.id is not None

    def test_get_or_create_idempotent(self, db_session: Session) -> None:
        """get_or_create returns the same record on repeated calls."""
        repo = AuthorRepository(db_session)
        first = repo.get_or_create("Jones, Mary")
        db_session.flush()
        second = repo.get_or_create("Jones, Mary")
        db_session.flush()
        assert first.id == second.id

    def test_link_and_get_authors_for_paper(self, db_session: Session) -> None:
        """link_to_paper + get_authors_for_paper round-trips author names."""
        a_repo = AuthorRepository(db_session)
        p_repo = PaperRepository(db_session)

        # Create a paper
        data = PaperCreate(
            title="Author Test Paper",
            contents="content",
            bibtex_id="AuthorTest001",
            bibtex="@article{AuthorTest001}",
            authors=["Doe, Jane", "Doe, John"],
        )
        summary = p_repo.add(data)
        db_session.flush()

        authors = a_repo.get_authors_for_paper(summary.id)
        assert set(authors) == {"Doe, Jane", "Doe, John"}


class TestPaperRepository:
    """Tests for PaperRepository."""

    def test_add_and_get_by_id(self, db_session: Session) -> None:
        """Added paper is retrievable by ID."""
        repo = PaperRepository(db_session)
        data = PaperCreate(
            title="My Paper",
            contents="Abstract text.",
            bibtex_id="MyPaper2024",
            bibtex="@article{MyPaper2024}",
            authors=["Author, A"],
        )
        summary = repo.add(data)
        db_session.flush()
        retrieved = repo.get_by_id(summary.id)
        assert retrieved is not None
        assert retrieved.title == "My Paper"
        assert "Author, A" in retrieved.authors

    def test_get_by_id_not_found(self, db_session: Session) -> None:
        """get_by_id returns None for a non-existent paper ID."""
        repo = PaperRepository(db_session)
        assert repo.get_by_id(999999) is None

    def test_add_duplicate_raises(self, db_session: Session) -> None:
        """Adding a paper with a duplicate bibtex_id raises ValueError."""
        repo = PaperRepository(db_session)
        data = PaperCreate(
            title="Dup Paper",
            contents="content",
            bibtex_id="DupKey2024",
            bibtex="@article{DupKey2024}",
            authors=[],
        )
        repo.add(data)
        db_session.flush()
        with pytest.raises(ValueError, match="already exists"):
            repo.add(data)

    def test_search_by_title_single_match(self, seeded_session: Session) -> None:
        """search_by_title finds a paper by substring match (single result)."""
        repo = PaperRepository(seeded_session)
        results = repo.search_by_title("BERT")
        assert len(results) == 1
        assert "BERT" in results[0].title

    def test_search_by_title_multiple_matches(self, seeded_session: Session) -> None:
        """search_by_title returns multiple papers when title substring matches several."""
        repo = PaperRepository(seeded_session)
        results = repo.search_by_title("Attention")
        assert len(results) >= 2

    def test_search_by_title_no_match(self, seeded_session: Session) -> None:
        """search_by_title returns empty list when no paper matches."""
        repo = PaperRepository(seeded_session)
        results = repo.search_by_title("ThisTitleDoesNotExist12345")
        assert results == []

    def test_search_by_author(self, seeded_session: Session) -> None:
        """search_by_author finds papers by author name substring."""
        repo = PaperRepository(seeded_session)
        results = repo.search_by_author("Vaswani")
        assert len(results) >= 1
        assert any("Vaswani" in a for r in results for a in r.authors)

    def test_search_by_author_no_match(self, seeded_session: Session) -> None:
        """search_by_author returns empty list when no author matches."""
        repo = PaperRepository(seeded_session)
        results = repo.search_by_author("NoSuchAuthorXYZ")
        assert results == []

    def test_delete_removes_paper(self, seeded_session: Session) -> None:
        """Deleted paper is no longer retrievable."""
        repo = PaperRepository(seeded_session)
        results = repo.search_by_title("BERT")
        assert results, "Need at least one paper to delete"
        paper_id = results[0].id
        repo.delete(paper_id)
        seeded_session.flush()
        assert repo.get_by_id(paper_id) is None

    def test_delete_not_found_raises(self, db_session: Session) -> None:
        """Deleting a non-existent paper raises LookupError."""
        repo = PaperRepository(db_session)
        with pytest.raises(LookupError):
            repo.delete(999999)

    def test_update_title(self, seeded_session: Session) -> None:
        """update_title changes the paper title."""
        repo = PaperRepository(seeded_session)
        results = repo.search_by_title("BERT")
        paper_id = results[0].id
        repo.update_title(paper_id, "BERT Updated")
        seeded_session.flush()
        updated = repo.get_by_id(paper_id)
        assert updated is not None
        assert updated.title == "BERT Updated"

    def test_update_contents(self, seeded_session: Session) -> None:
        """update_contents changes the paper summary."""
        repo = PaperRepository(seeded_session)
        results = repo.search_by_title("BERT")
        paper_id = results[0].id
        repo.update_contents(paper_id, "New summary text.")
        seeded_session.flush()
        updated = repo.get_by_id(paper_id)
        assert updated is not None
        assert updated.contents == "New summary text."

    def test_update_author(self, seeded_session: Session) -> None:
        """update_author replaces all authors with a single new author."""
        repo = PaperRepository(seeded_session)
        results = repo.search_by_title("BERT")
        paper_id = results[0].id
        repo.update_author(paper_id, "NewAuthor, Test")
        seeded_session.flush()
        updated = repo.get_by_id(paper_id)
        assert updated is not None
        assert updated.authors == ["NewAuthor, Test"]
