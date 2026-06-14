"""Integration tests for the paper_sorts persistence layer.

Tests run against a real ephemeral PostgreSQL instance (not mocked).
All assertions reference seed data defined in tests/fixtures/seed_papers.py.
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


class TestPaperRepository:
    """Tests for PaperRepository CRUD and search methods."""

    def test_create_paper(self, db_url: str) -> None:
        """Creating a paper inserts it and returns a PaperSummary with correct fields."""
        data = PaperCreate(
            title="Test Paper",
            contents="Test contents",
            bibtex_id="TestKey2024",
            bibtex="@article{TestKey2024, title={Test Paper}}",
            authors=["Test, Author"],
        )
        with with_session(db_url) as session:
            repo = PaperRepository(session)
            result = repo.create(data)
        assert result.title == "Test Paper"
        assert result.bibtex_id == "TestKey2024"
        assert "Test, Author" in result.authors
        # Cleanup
        with with_session(db_url) as session:
            PaperRepository(session).delete("TestKey2024")

    def test_create_duplicate_raises(self, seeded_db_url: str) -> None:
        """Creating a paper with an existing bibtex_id raises ValueError."""
        data = PaperCreate(
            title="Duplicate",
            contents="...",
            bibtex_id="Wang2021LargeScaleSA",  # already in seed
            bibtex="@article{Wang2021LargeScaleSA}",
            authors=["Wang, Changhan"],
        )
        with pytest.raises(ValueError, match="already exists"):
            with with_session(seeded_db_url) as session:
                PaperRepository(session).create(data)

    def test_search_by_title_exact(self, seeded_db_url: str) -> None:
        """Searching for a unique title fragment returns exactly one result."""
        with with_session(seeded_db_url) as session:
            results = PaperRepository(session).search_by_title("Large-scale")
        assert len(results) == 1
        assert results[0].bibtex_id == "Wang2021LargeScaleSA"

    def test_search_by_title_multiple(self, seeded_db_url: str) -> None:
        """Searching for a common title word returns multiple results."""
        with with_session(seeded_db_url) as session:
            results = PaperRepository(session).search_by_title("speech")
        # Wang2021 and Lee2021 both contain "speech" in title
        bibtex_ids = {r.bibtex_id for r in results}
        assert "Wang2021LargeScaleSA" in bibtex_ids
        assert "Lee2021Direct" in bibtex_ids

    def test_search_by_title_no_results(self, seeded_db_url: str) -> None:
        """Searching for a non-existent title returns an empty list."""
        with with_session(seeded_db_url) as session:
            results = PaperRepository(session).search_by_title("xyzzy_nonexistent")
        assert results == []

    def test_search_by_author(self, seeded_db_url: str) -> None:
        """Searching by a shared author returns all their papers."""
        with with_session(seeded_db_url) as session:
            results = PaperRepository(session).search_by_author("Pino")
        # Both Wang2021 and Lee2021 have Pino, J. as author
        bibtex_ids = {r.bibtex_id for r in results}
        assert "Wang2021LargeScaleSA" in bibtex_ids
        assert "Lee2021Direct" in bibtex_ids

    def test_search_by_author_no_results_raises(self, seeded_db_url: str) -> None:
        """Searching by a non-existent author raises KeyError."""
        with pytest.raises(KeyError):
            with with_session(seeded_db_url) as session:
                PaperRepository(session).search_by_author("Xyzzy_NoSuchAuthor")

    def test_get_by_bibtex_id(self, seeded_db_url: str) -> None:
        """Fetching a paper by bibtex_id returns the correct PaperSummary."""
        with with_session(seeded_db_url) as session:
            result = PaperRepository(session).get_by_bibtex_id("Smith2022Survey")
        assert result is not None
        assert result.title == "A survey of transformer architectures for NLP"

    def test_get_by_bibtex_id_missing(self, seeded_db_url: str) -> None:
        """Fetching a non-existent bibtex_id returns None."""
        with with_session(seeded_db_url) as session:
            result = PaperRepository(session).get_by_bibtex_id("NoSuchKey9999")
        assert result is None

    def test_update_title(self, seeded_db_url: str) -> None:
        """Updating title stores the new value and returns updated DTO."""
        with with_session(seeded_db_url) as session:
            result = PaperRepository(session).update_title(
                "Smith2022Survey", "Updated Survey Title"
            )
        assert result.title == "Updated Survey Title"
        # Verify persisted
        with with_session(seeded_db_url) as session:
            fetched = PaperRepository(session).get_by_bibtex_id("Smith2022Survey")
        assert fetched is not None
        assert fetched.title == "Updated Survey Title"

    def test_update_contents(self, seeded_db_url: str) -> None:
        """Updating contents stores the new value."""
        with with_session(seeded_db_url) as session:
            result = PaperRepository(session).update_contents(
                "Smith2022Survey", "New summary text"
            )
        assert result.contents == "New summary text"

    def test_delete_paper(self, db_url: str) -> None:
        """Deleting a paper removes it, its BibTeX entry, and orphan authors."""
        data = PaperCreate(
            title="Temporary Paper",
            contents="Will be deleted",
            bibtex_id="TempKey2024",
            bibtex="@article{TempKey2024, title={Temporary Paper}}",
            authors=["Temp, Author"],
        )
        with with_session(db_url) as session:
            PaperRepository(session).create(data)
        with with_session(db_url) as session:
            title = PaperRepository(session).delete("TempKey2024")
        assert title == "Temporary Paper"
        with with_session(db_url) as session:
            result = PaperRepository(session).get_by_bibtex_id("TempKey2024")
        assert result is None

    def test_delete_paper_not_found(self, db_url: str) -> None:
        """Deleting a non-existent paper raises KeyError."""
        with pytest.raises(KeyError):
            with with_session(db_url) as session:
                PaperRepository(session).delete("NoSuchKey9999")

    def test_delete_preserves_shared_authors(self, seeded_db_url: str) -> None:
        """Deleting Wang2021 does not remove Pino, J. (who also authors Lee2021)."""
        # Make a copy so seeded_db_url still has original after test
        data = PaperCreate(
            title="Wang Copy",
            contents="Copy for delete test",
            bibtex_id="WangCopy2024",
            bibtex="@article{WangCopy2024}",
            authors=["Wang, Changhan", "Pino, J."],
        )
        with with_session(seeded_db_url) as session:
            PaperRepository(session).create(data)
        with with_session(seeded_db_url) as session:
            PaperRepository(session).delete("WangCopy2024")
        # Pino, J. should still exist (linked to Lee2021)
        with with_session(seeded_db_url) as session:
            author = AuthorRepository(session).get_by_name("Pino, J.")
        assert author is not None


class TestBibRepository:
    """Tests for BibRepository operations."""

    def test_get_or_create_creates(self, db_url: str) -> None:
        """get_or_create creates a new BibEntry when key doesn't exist."""
        bibtex_id_value: str = ""
        with with_session(db_url) as session:
            bib_repo = BibRepository(session)
            entry = bib_repo.get_or_create("TestBib2024", "@article{TestBib2024}")
            bibtex_id_value = entry.bibtex_id  # read inside session
        assert bibtex_id_value == "TestBib2024"
        # Cleanup
        with with_session(db_url) as session:
            bib = BibRepository(session).get_by_id("TestBib2024")
            if bib:
                session.delete(bib)

    def test_update_bibtex(self, seeded_db_url: str) -> None:
        """Updating the bibtex string persists the new value."""
        new_bibtex = "@article{Wang2021LargeScaleSA, note={updated}}"
        with with_session(seeded_db_url) as session:
            BibRepository(session).update("Wang2021LargeScaleSA", new_bibtex)
        with with_session(seeded_db_url) as session:
            entry = BibRepository(session).get_by_id("Wang2021LargeScaleSA")
            assert entry is not None
            assert entry.bibtex == new_bibtex

    def test_update_bibtex_duplicate_raises(self, seeded_db_url: str) -> None:
        """Updating bibtex to an already-existing string raises ValueError."""
        # Lee2021's bibtex already exists; trying to set Wang2021 to it should fail
        lee_bibtex: str = ""
        with with_session(seeded_db_url) as session:
            lee_entry = BibRepository(session).get_by_id("Lee2021Direct")
            assert lee_entry is not None
            lee_bibtex = lee_entry.bibtex or ""
        with pytest.raises(ValueError, match="already exists"):
            with with_session(seeded_db_url) as session:
                BibRepository(session).update("Wang2021LargeScaleSA", lee_bibtex)


class TestAuthorRepository:
    """Tests for AuthorRepository operations."""

    def test_get_or_create_creates(self, db_url: str) -> None:
        """get_or_create creates a new Author when name doesn't exist."""
        author_name: str = ""
        with with_session(db_url) as session:
            repo = AuthorRepository(session)
            author = repo.get_or_create("New, Author")
            author_name = author.author or ""  # read inside session
        assert author_name == "New, Author"
        # Cleanup
        with with_session(db_url) as session:
            a = AuthorRepository(session).get_by_name("New, Author")
            if a:
                session.delete(a)

    def test_delete_orphans(self, db_url: str) -> None:
        """delete_orphans removes authors with no linked papers."""
        with with_session(db_url) as session:
            repo = AuthorRepository(session)
            repo.get_or_create("Orphan, Author")  # no paper link
        with with_session(db_url) as session:
            count = AuthorRepository(session).delete_orphans()
        assert count >= 1

    def test_update_name_renames(self, seeded_db_url: str) -> None:
        """Renaming an author updates the name in the database."""
        with with_session(seeded_db_url) as session:
            AuthorRepository(session).update_name("Smith, John", "Smith, Jonathan")
        with with_session(seeded_db_url) as session:
            updated = AuthorRepository(session).get_by_name("Smith, Jonathan")
        assert updated is not None
        # Restore for other tests
        with with_session(seeded_db_url) as session:
            AuthorRepository(session).update_name("Smith, Jonathan", "Smith, John")


class TestMigration:
    """Tests for Alembic migration idempotency."""

    def test_migrations_idempotent(self, migrated_db_url: str) -> None:
        """Running migrations twice leaves the schema in the same state."""
        import os

        from alembic import command
        from alembic.config import Config

        os.environ["PDBSEARCH_DATABASE_URL"] = migrated_db_url
        alembic_cfg = Config("alembic.ini")
        alembic_cfg.set_main_option("sqlalchemy.url", migrated_db_url)
        # Second upgrade should be a no-op
        command.upgrade(alembic_cfg, "head")

        # Verify schema is still intact by inserting and retrieving a row
        data = PaperCreate(
            title="Idempotency Test",
            contents="Testing migration idempotency",
            bibtex_id="IdempTest2024",
            bibtex="@article{IdempTest2024}",
            authors=["Test, Idem"],
        )
        with with_session(migrated_db_url) as session:
            result = PaperRepository(session).create(data)
        assert result.bibtex_id == "IdempTest2024"
        # Cleanup
        with with_session(migrated_db_url) as session:
            PaperRepository(session).delete("IdempTest2024")
