"""Integration tests for the paper_sorts service layer.

Tests run against a real ephemeral PostgreSQL instance.
Services are tested end-to-end through the full stack
(service → repository → ORM → real DB).
"""

from __future__ import annotations

import pytest

from paper_sorts.db.repositories import PaperCreate
from paper_sorts.services.paper_service import (
    add_paper,
    delete_paper,
    search_by_author,
    search_by_title,
    update_field,
)


class TestSearchByTitle:
    """Tests for paper_service.search_by_title."""

    def test_exact_match(self, seeded_db_url: str) -> None:
        """Searching for a unique title fragment returns one result."""
        results = search_by_title(seeded_db_url, "Large-scale")
        assert len(results) == 1
        assert results[0].bibtex_id == "Wang2021LargeScaleSA"

    def test_multiple_matches(self, seeded_db_url: str) -> None:
        """Searching for a common word returns multiple results."""
        results = search_by_title(seeded_db_url, "speech")
        ids = {r.bibtex_id for r in results}
        assert "Wang2021LargeScaleSA" in ids
        assert "Lee2021Direct" in ids

    def test_no_match_returns_empty(self, seeded_db_url: str) -> None:
        """Searching for a non-existent title returns an empty list."""
        results = search_by_title(seeded_db_url, "xyzzy_nobody_wrote_this")
        assert results == []

    def test_result_includes_authors_and_bibtex(self, seeded_db_url: str) -> None:
        """Search result includes author list and bibtex string."""
        results = search_by_title(seeded_db_url, "Large-scale")
        assert len(results) == 1
        assert "Wang, Changhan" in results[0].authors
        assert "Pino, J." in results[0].authors
        assert "@article" in results[0].bibtex


class TestSearchByAuthor:
    """Tests for paper_service.search_by_author."""

    def test_shared_author(self, seeded_db_url: str) -> None:
        """Searching by a shared author returns all their papers."""
        results = search_by_author(seeded_db_url, "Pino")
        ids = {r.bibtex_id for r in results}
        assert "Wang2021LargeScaleSA" in ids
        assert "Lee2021Direct" in ids

    def test_unique_author(self, seeded_db_url: str) -> None:
        """Searching by a unique author returns only their papers."""
        results = search_by_author(seeded_db_url, "Smith")
        assert len(results) == 1
        assert results[0].bibtex_id == "Smith2022Survey"

    def test_no_author_raises(self, seeded_db_url: str) -> None:
        """Searching by a non-existent author raises KeyError."""
        with pytest.raises(KeyError):
            search_by_author(seeded_db_url, "xyzzy_no_such_person")


class TestAddPaper:
    """Tests for paper_service.add_paper."""

    def test_add_new_paper(self, db_url: str) -> None:
        """Adding a new paper returns a PaperSummary with correct fields."""
        data = PaperCreate(
            title="New Test Paper",
            contents="Test contents for service test",
            bibtex_id="NewPaper2024",
            bibtex="@article{NewPaper2024, title={New Test Paper}}",
            authors=["New, Author", "Second, Author"],
        )
        result = add_paper(db_url, data)
        assert result.title == "New Test Paper"
        assert "New, Author" in result.authors
        # Cleanup
        delete_paper(db_url, "NewPaper2024")

    def test_add_duplicate_raises(self, seeded_db_url: str) -> None:
        """Adding a paper with a duplicate bibtex_id raises ValueError."""
        data = PaperCreate(
            title="Duplicate",
            contents="...",
            bibtex_id="Wang2021LargeScaleSA",
            bibtex="@article{Wang2021LargeScaleSA, note={dup}}",
            authors=["Wang, Changhan"],
        )
        with pytest.raises(ValueError):
            add_paper(seeded_db_url, data)


class TestUpdateField:
    """Tests for paper_service.update_field."""

    def test_update_title(self, seeded_db_url: str) -> None:
        """Updating 'title' changes the paper title."""
        result = update_field(seeded_db_url, "Smith2022Survey", "title", "New Survey Title")
        assert result.title == "New Survey Title"
        # Verify with search
        found = search_by_title(seeded_db_url, "New Survey Title")
        assert len(found) == 1

    def test_update_contents(self, seeded_db_url: str) -> None:
        """Updating 'contents' changes the paper summary."""
        result = update_field(
            seeded_db_url, "Smith2022Survey", "contents", "Updated summary"
        )
        assert result.contents == "Updated summary"

    def test_update_bibtex(self, seeded_db_url: str) -> None:
        """Updating 'bibtex' changes the BibTeX source."""
        new_bib = "@article{Smith2022Survey, note={updated bibtex}}"
        result = update_field(seeded_db_url, "Smith2022Survey", "bibtex", new_bib)
        assert result.bibtex == new_bib

    def test_update_author(self, seeded_db_url: str) -> None:
        """Updating 'author' renames the author across all their papers."""
        update_field(
            seeded_db_url, "Smith2022Survey", "author", "Smith, John -> Smith, Jonathan"
        )
        results = search_by_author(seeded_db_url, "Smith, Jonathan")
        assert len(results) >= 1
        # Restore
        update_field(
            seeded_db_url, "Smith2022Survey", "author", "Smith, Jonathan -> Smith, John"
        )

    def test_update_nonexistent_raises(self, seeded_db_url: str) -> None:
        """Updating a non-existent paper raises KeyError."""
        with pytest.raises(KeyError):
            update_field(seeded_db_url, "NoSuchKey9999", "title", "Anything")


class TestDeletePaper:
    """Tests for paper_service.delete_paper."""

    def test_delete_returns_title(self, db_url: str) -> None:
        """Deleting a paper returns its title."""
        data = PaperCreate(
            title="To Be Deleted",
            contents="...",
            bibtex_id="DeleteMe2024",
            bibtex="@article{DeleteMe2024}",
            authors=["Delete, Me"],
        )
        add_paper(db_url, data)
        title = delete_paper(db_url, "DeleteMe2024")
        assert title == "To Be Deleted"

    def test_delete_makes_paper_unfindable(self, db_url: str) -> None:
        """After deleting, searching for the paper returns nothing."""
        data = PaperCreate(
            title="Gone Paper",
            contents="...",
            bibtex_id="GoneKey2024",
            bibtex="@article{GoneKey2024}",
            authors=["Gone, Author"],
        )
        add_paper(db_url, data)
        delete_paper(db_url, "GoneKey2024")
        results = search_by_title(db_url, "Gone Paper")
        assert results == []

    def test_delete_nonexistent_raises(self, seeded_db_url: str) -> None:
        """Deleting a non-existent paper raises KeyError."""
        with pytest.raises(KeyError):
            delete_paper(seeded_db_url, "NoSuchKey9999")
