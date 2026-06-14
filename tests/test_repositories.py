"""Integration tests for the persistence layer (db/repositories.py).

All tests run against an ephemeral PostgreSQL instance via pytest-postgresql.
No mocking of SQLAlchemy session, repositories, or database driver (constitution II).

Seed data used is SEED_PAPERS from tests/fixtures/seed_papers.py:
- Paper 1 (Vaswani2017AttentionIA): unique title, 2 authors
- Paper 2 (Devlin2019BERT): shared title, 1 author
- Paper 3 (Cui2020BERT): shared title with Paper 2, 2 authors (one shared with Paper 1)
- Paper 4 (Brown2020GPT3): unique title, 1 author
"""

from __future__ import annotations

import pytest
from sqlalchemy import Engine

from paper_sorts.db.repositories import (
    AuthorRepository,
    BibRepository,
    PaperCreate,
    PaperRepository,
)
from paper_sorts.db.session import with_session


class TestBibRepository:
    """Tests for BibRepository CRUD operations."""

    def test_create_and_get(self, clean_engine: Engine) -> None:
        """Creating a bib entry allows retrieval by bibtex_id."""
        with with_session(clean_engine) as session:
            repo = BibRepository(session)
            bib = repo.create("TestKey2024", "@article{TestKey2024}")
            assert bib.bibtex_id == "TestKey2024"

        with with_session(clean_engine) as session:
            repo = BibRepository(session)
            fetched = repo.get_by_id("TestKey2024")
            assert fetched is not None
            assert fetched.bibtex == "@article{TestKey2024}"

    def test_exists_true(self, seeded_engine: Engine) -> None:
        """exists() returns True for a known bibtex_id."""
        with with_session(seeded_engine) as session:
            repo = BibRepository(session)
            assert repo.exists("Vaswani2017AttentionIA") is True

    def test_exists_false(self, seeded_engine: Engine) -> None:
        """exists() returns False for an unknown bibtex_id."""
        with with_session(seeded_engine) as session:
            repo = BibRepository(session)
            assert repo.exists("NonExistentKey") is False

    def test_update(self, seeded_engine: Engine) -> None:
        """Updating a bib entry persists the new bibtex string."""
        with with_session(seeded_engine) as session:
            repo = BibRepository(session)
            repo.update("Brown2020GPT3", "@article{Brown2020GPT3, new=true}")

        with with_session(seeded_engine) as session:
            repo = BibRepository(session)
            bib = repo.get_by_id("Brown2020GPT3")
            assert bib is not None
            assert "new=true" in bib.bibtex

    def test_update_nonexistent_raises(self, clean_engine: Engine) -> None:
        """Updating a non-existent bibtex_id raises ValueError."""
        with pytest.raises(ValueError, match="not found in bib table"):
            with with_session(clean_engine) as session:
                BibRepository(session).update("NoSuchKey", "...")

    def test_delete(self, clean_engine: Engine) -> None:
        """Deleting a bib entry removes it from the table."""
        with with_session(clean_engine) as session:
            BibRepository(session).create("ToDelete2024", "@misc{ToDelete2024}")

        with with_session(clean_engine) as session:
            BibRepository(session).delete("ToDelete2024")

        with with_session(clean_engine) as session:
            assert BibRepository(session).get_by_id("ToDelete2024") is None


class TestPaperRepository:
    """Tests for PaperRepository search and CRUD operations."""

    def test_search_by_title_unique(self, seeded_engine: Engine) -> None:
        """search_by_title returns exactly one result for a unique title."""
        with with_session(seeded_engine) as session:
            results = PaperRepository(session).search_by_title("Attention Is All You Need")
        assert len(results) == 1
        assert results[0].bibtex_id == "Vaswani2017AttentionIA"
        assert set(results[0].authors) == {"Vaswani, Ashish", "Shazeer, Noam"}

    def test_search_by_title_multiple(self, seeded_engine: Engine) -> None:
        """search_by_title returns multiple results for an ambiguous title."""
        with with_session(seeded_engine) as session:
            results = PaperRepository(session).search_by_title(
                "BERT: Pre-training of Deep Bidirectional Transformers"
            )
        assert len(results) == 2
        bibtex_ids = {r.bibtex_id for r in results}
        assert bibtex_ids == {"Devlin2019BERT", "Cui2020BERT"}

    def test_search_by_title_no_match(self, seeded_engine: Engine) -> None:
        """search_by_title returns empty list when title not found."""
        with with_session(seeded_engine) as session:
            results = PaperRepository(session).search_by_title("Nonexistent Title XYZ")
        assert results == []

    def test_search_by_author(self, seeded_engine: Engine) -> None:
        """search_by_author returns all papers by the given author."""
        with with_session(seeded_engine) as session:
            results = PaperRepository(session).search_by_author("Vaswani, Ashish")
        assert len(results) == 2
        bibtex_ids = {r.bibtex_id for r in results}
        assert bibtex_ids == {"Vaswani2017AttentionIA", "Cui2020BERT"}

    def test_search_by_author_no_match(self, seeded_engine: Engine) -> None:
        """search_by_author returns empty list for unknown author."""
        with with_session(seeded_engine) as session:
            results = PaperRepository(session).search_by_author("Nobody, None")
        assert results == []

    def test_create_and_get_by_bibtex_id(self, clean_engine: Engine) -> None:
        """Creating a paper allows retrieval by bibtex_id."""
        paper_data = PaperCreate(
            title="Test Paper",
            contents="A test.",
            bibtex_id="Test2024",
            bibtex="@misc{Test2024}",
            authors=["Author, First"],
        )
        with with_session(clean_engine) as session:
            PaperRepository(session).create(paper_data)

        with with_session(clean_engine) as session:
            summary = PaperRepository(session).get_by_bibtex_id("Test2024")
        assert summary is not None
        assert summary.title == "Test Paper"
        assert summary.authors == ["Author, First"]

    def test_create_duplicate_raises(self, seeded_engine: Engine) -> None:
        """Creating a paper with an existing bibtex_id raises ValueError."""
        duplicate = PaperCreate(
            title="Another Paper",
            contents="Duplicate.",
            bibtex_id="Vaswani2017AttentionIA",  # already exists
            bibtex="@misc{Vaswani2017AttentionIA}",
            authors=["Author, X"],
        )
        with pytest.raises(ValueError, match="already exists"):
            with with_session(seeded_engine) as session:
                PaperRepository(session).create(duplicate)

    def test_update_title(self, seeded_engine: Engine) -> None:
        """update_title persists the new title."""
        with with_session(seeded_engine) as session:
            repo = PaperRepository(session)
            paper = repo.get_by_bibtex_id("Brown2020GPT3")
            assert paper is not None
            repo.update_title(paper.paper_id, "GPT-3 Updated Title")

        with with_session(seeded_engine) as session:
            updated = PaperRepository(session).get_by_bibtex_id("Brown2020GPT3")
            assert updated is not None
            assert updated.title == "GPT-3 Updated Title"

    def test_update_contents(self, seeded_engine: Engine) -> None:
        """update_contents persists the new summary."""
        with with_session(seeded_engine) as session:
            repo = PaperRepository(session)
            paper = repo.get_by_bibtex_id("Brown2020GPT3")
            assert paper is not None
            repo.update_contents(paper.paper_id, "Updated summary.")

        with with_session(seeded_engine) as session:
            updated = PaperRepository(session).get_by_bibtex_id("Brown2020GPT3")
            assert updated is not None
            assert updated.contents == "Updated summary."

    def test_delete_removes_paper_and_bib(self, clean_engine: Engine) -> None:
        """Deleting a paper removes the paper row and its bib entry."""
        paper_data = PaperCreate(
            title="To Delete",
            contents="Ephemeral.",
            bibtex_id="Delete2024",
            bibtex="@misc{Delete2024}",
            authors=["Delete, Author"],
        )
        with with_session(clean_engine) as session:
            repo = PaperRepository(session)
            summary = repo.create(paper_data)
            paper_id = summary.paper_id

        with with_session(clean_engine) as session:
            PaperRepository(session).delete(paper_id)

        with with_session(clean_engine) as session:
            assert PaperRepository(session).get_by_bibtex_id("Delete2024") is None
            assert BibRepository(session).get_by_id("Delete2024") is None

    def test_delete_nonexistent_raises(self, clean_engine: Engine) -> None:
        """Deleting a non-existent paper_id raises ValueError."""
        with pytest.raises(ValueError, match="not found"):
            with with_session(clean_engine) as session:
                PaperRepository(session).delete(99999)


class TestAuthorRepository:
    """Tests for AuthorRepository operations."""

    def test_get_or_create_new(self, clean_engine: Engine) -> None:
        """get_or_create inserts a new author when not present."""
        with with_session(clean_engine) as session:
            repo = AuthorRepository(session)
            author = repo.get_or_create("New, Author")
            assert author.id is not None
            assert author.author == "New, Author"

    def test_get_or_create_existing(self, seeded_engine: Engine) -> None:
        """get_or_create returns existing author without creating a duplicate."""
        with with_session(seeded_engine) as session:
            repo = AuthorRepository(session)
            a1 = repo.get_or_create("Vaswani, Ashish")
            a2 = repo.get_or_create("Vaswani, Ashish")
            assert a1.id == a2.id

    def test_get_authors_for_paper(self, seeded_engine: Engine) -> None:
        """get_authors_for_paper returns all authors linked to a paper."""
        with with_session(seeded_engine) as session:
            paper = PaperRepository(session).get_by_bibtex_id("Vaswani2017AttentionIA")
            assert paper is not None
            authors = AuthorRepository(session).get_authors_for_paper(paper.paper_id)
        assert set(authors) == {"Vaswani, Ashish", "Shazeer, Noam"}
