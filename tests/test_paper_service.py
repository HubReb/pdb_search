"""Integration tests for paper_service functions.

Exercises the service layer against a real ephemeral PostgreSQL database.
No mocking of the session or repository per the project constitution.
"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import IntegrityError

from paper_sorts.db.repositories import PaperCreate
from paper_sorts.services import paper_service


class TestSearchByTitle:
    """Tests for paper_service.search_by_title."""

    def test_found(self, seeded_engine: object) -> None:
        """Service returns correct paper when searched by title."""
        results = paper_service.search_by_title(
            seeded_engine,  # type: ignore[arg-type]
            "Large-scale Self- and Semi-Supervised learning for speech translation",
        )
        assert len(results) == 1
        assert results[0].bibtex_id == "Wang2021LargeScaleSA"
        assert "Pino, J." in results[0].authors

    def test_not_found(self, seeded_engine: object) -> None:
        """Service returns empty list for an unknown title."""
        results = paper_service.search_by_title(seeded_engine, "not in db")  # type: ignore[arg-type]
        assert results == []


class TestSearchByAuthor:
    """Tests for paper_service.search_by_author."""

    def test_found(self, seeded_engine: object) -> None:
        """Service returns papers for a known author."""
        results = paper_service.search_by_author(seeded_engine, "Pino, J.")  # type: ignore[arg-type]
        assert len(results) >= 1
        titles = {p.title for p in results}
        assert "Large-scale Self- and Semi-Supervised learning for speech translation" in titles

    def test_not_found(self, seeded_engine: object) -> None:
        """Service returns empty list for an unknown author."""
        results = paper_service.search_by_author(seeded_engine, "Nobody Known")  # type: ignore[arg-type]
        assert results == []


class TestAddPaper:
    """Tests for paper_service.add_paper."""

    def test_add_and_retrieve(self, engine: object) -> None:
        """Added paper is retrievable by both title and author."""
        paper = PaperCreate(
            title="Service Add Test",
            contents="Service layer test summary.",
            bibtex_id="SvcAdd2024",
            bibtex="@article{SvcAdd2024, title={Service Add Test}, year={2024}}",
            authors=["Service, Author"],
        )
        result = paper_service.add_paper(engine, paper)  # type: ignore[arg-type]
        assert result.id > 0
        assert result.title == "Service Add Test"

        by_title = paper_service.search_by_title(engine, "Service Add Test")  # type: ignore[arg-type]
        assert len(by_title) == 1

        by_author = paper_service.search_by_author(engine, "Service, Author")  # type: ignore[arg-type]
        assert len(by_author) == 1

        # Cleanup
        paper_service.delete_paper(engine, result.id)  # type: ignore[arg-type]

    def test_duplicate_raises(self, engine: object) -> None:
        """Adding a duplicate bibtex_id raises IntegrityError."""
        paper = PaperCreate(
            title="Dup Service",
            contents="dup.",
            bibtex_id="DupSvc2024",
            bibtex="@article{DupSvc2024, title={Dup}, year={2024}}",
            authors=["Dup, A."],
        )
        result = paper_service.add_paper(engine, paper)  # type: ignore[arg-type]
        try:
            with pytest.raises(IntegrityError):
                paper_service.add_paper(engine, paper)  # type: ignore[arg-type]
        finally:
            paper_service.delete_paper(engine, result.id)  # type: ignore[arg-type]


class TestUpdateField:
    """Tests for paper_service.update_field."""

    def test_update_title(self, engine: object) -> None:
        """Service can update the title of a paper."""
        paper = PaperCreate(
            title="Before Update",
            contents="Content.",
            bibtex_id="SvcUpd2024",
            bibtex="@article{SvcUpd2024, year={2024}}",
            authors=["Upd, A."],
        )
        result = paper_service.add_paper(engine, paper)  # type: ignore[arg-type]
        try:
            paper_service.update_field(
                engine, result.id, "papers", "title", "After Update"  # type: ignore[arg-type]
            )
            updated = paper_service.search_by_title(engine, "After Update")  # type: ignore[arg-type]
            assert len(updated) == 1
        finally:
            results = paper_service.search_by_title(engine, "After Update")  # type: ignore[arg-type]
            if results:
                paper_service.delete_paper(engine, results[0].id)  # type: ignore[arg-type]


class TestDeletePaper:
    """Tests for paper_service.delete_paper."""

    def test_delete(self, engine: object) -> None:
        """Deleting a paper removes it from the database."""
        paper = PaperCreate(
            title="SvcDel Test",
            contents="Del.",
            bibtex_id="SvcDel2024",
            bibtex="@article{SvcDel2024, year={2024}}",
            authors=["Del, A."],
        )
        result = paper_service.add_paper(engine, paper)  # type: ignore[arg-type]
        paper_service.delete_paper(engine, result.id)  # type: ignore[arg-type]
        remaining = paper_service.search_by_title(engine, "SvcDel Test")  # type: ignore[arg-type]
        assert remaining == []
