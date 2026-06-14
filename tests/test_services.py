"""Integration tests for src/paper_sorts/services/paper_service.py.

All tests run against an ephemeral PostgreSQL instance.
No mocking of SQLAlchemy session or repositories (constitution II).

Seed data: SEED_PAPERS from tests/fixtures/seed_papers.py
"""

from __future__ import annotations

import pytest
from sqlalchemy import Engine

from paper_sorts.db.repositories import PaperCreate, PaperRepository
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service


class TestSearchByTitle:
    """Tests for paper_service.search_by_title."""

    def test_unique_title_returns_one(self, seeded_engine: Engine) -> None:
        """search_by_title returns one result for a unique title."""
        with with_session(seeded_engine) as session:
            results = paper_service.search_by_title(session, "Attention Is All You Need")
        assert len(results) == 1
        assert results[0].bibtex_id == "Vaswani2017AttentionIA"

    def test_ambiguous_title_returns_multiple(self, seeded_engine: Engine) -> None:
        """search_by_title returns multiple results for a shared title."""
        with with_session(seeded_engine) as session:
            results = paper_service.search_by_title(
                session, "BERT: Pre-training of Deep Bidirectional Transformers"
            )
        assert len(results) == 2

    def test_no_match_returns_empty(self, seeded_engine: Engine) -> None:
        """search_by_title returns empty list when no match."""
        with with_session(seeded_engine) as session:
            results = paper_service.search_by_title(session, "Nonexistent XYZ")
        assert results == []


class TestSearchByAuthor:
    """Tests for paper_service.search_by_author."""

    def test_known_author_returns_papers(self, seeded_engine: Engine) -> None:
        """search_by_author returns all papers for a known author."""
        with with_session(seeded_engine) as session:
            results = paper_service.search_by_author(session, "Vaswani, Ashish")
        assert len(results) == 2

    def test_unknown_author_returns_empty(self, seeded_engine: Engine) -> None:
        """search_by_author returns empty list for unknown author."""
        with with_session(seeded_engine) as session:
            results = paper_service.search_by_author(session, "Ghost, Nobody")
        assert results == []


class TestAddPaper:
    """Tests for paper_service.add_paper."""

    def test_add_new_paper_persisted(self, clean_engine: Engine) -> None:
        """add_paper inserts a paper that is subsequently retrievable."""
        paper = PaperCreate(
            title="New Paper Title",
            contents="Summary here.",
            bibtex_id="NewPaper2024",
            bibtex="@misc{NewPaper2024}",
            authors=["Last, First"],
        )
        with with_session(clean_engine) as session:
            summary = paper_service.add_paper(session, paper)
        assert summary.bibtex_id == "NewPaper2024"
        assert summary.title == "New Paper Title"
        assert "Last, First" in summary.authors

        # Verify persistence
        with with_session(clean_engine) as session:
            retrieved = PaperRepository(session).get_by_bibtex_id("NewPaper2024")
        assert retrieved is not None

    def test_add_duplicate_bibtex_id_raises(self, seeded_engine: Engine) -> None:
        """add_paper raises ValueError when bibtex_id already exists."""
        duplicate = PaperCreate(
            title="Duplicate",
            contents="Duplicate.",
            bibtex_id="Vaswani2017AttentionIA",  # already seeded
            bibtex="@misc{dup}",
            authors=["Dup, Author"],
        )
        with pytest.raises(ValueError, match="already exists"):
            with with_session(seeded_engine) as session:
                paper_service.add_paper(session, duplicate)

    def test_add_paper_searchable_by_author(self, clean_engine: Engine) -> None:
        """Added paper is immediately searchable by author."""
        paper = PaperCreate(
            title="Author Search Test",
            contents="Test.",
            bibtex_id="AuthSearch2024",
            bibtex="@misc{AuthSearch2024}",
            authors=["Search, Auth"],
        )
        with with_session(clean_engine) as session:
            paper_service.add_paper(session, paper)

        with with_session(clean_engine) as session:
            results = paper_service.search_by_author(session, "Search, Auth")
        assert len(results) == 1


class TestUpdateField:
    """Tests for paper_service.update_field."""

    def _get_paper_id(self, engine: Engine, bibtex_id: str) -> int:
        with with_session(engine) as session:
            paper = PaperRepository(session).get_by_bibtex_id(bibtex_id)
            assert paper is not None
            return paper.paper_id

    def test_update_title(self, seeded_engine: Engine) -> None:
        """update_field with table='papers', column='title' updates the title."""
        paper_id = self._get_paper_id(seeded_engine, "Brown2020GPT3")
        with with_session(seeded_engine) as session:
            paper_service.update_field(session, paper_id, "papers", "title", "New GPT-3 Title")

        with with_session(seeded_engine) as session:
            updated = PaperRepository(session).get_by_bibtex_id("Brown2020GPT3")
            assert updated is not None
            assert updated.title == "New GPT-3 Title"

    def test_update_contents(self, seeded_engine: Engine) -> None:
        """update_field with table='papers', column='contents' updates summary."""
        paper_id = self._get_paper_id(seeded_engine, "Brown2020GPT3")
        with with_session(seeded_engine) as session:
            paper_service.update_field(session, paper_id, "papers", "contents", "New summary.")

        with with_session(seeded_engine) as session:
            updated = PaperRepository(session).get_by_bibtex_id("Brown2020GPT3")
            assert updated is not None
            assert updated.contents == "New summary."

    def test_update_bibtex(self, seeded_engine: Engine) -> None:
        """update_field with table='bib', column='bibtex' updates BibTeX string."""
        paper_id = self._get_paper_id(seeded_engine, "Brown2020GPT3")
        with with_session(seeded_engine) as session:
            paper_service.update_field(
                session, paper_id, "bib", "bibtex", "@article{Brown2020GPT3, updated=true}"
            )

        with with_session(seeded_engine) as session:
            updated = PaperRepository(session).get_by_bibtex_id("Brown2020GPT3")
            assert updated is not None
            assert "updated=true" in updated.bibtex

    def test_update_nonexistent_paper_raises(self, clean_engine: Engine) -> None:
        """update_field raises ValueError for non-existent paper_id."""
        with pytest.raises(ValueError, match="not found"):
            with with_session(clean_engine) as session:
                paper_service.update_field(session, 99999, "papers", "title", "X")

    def test_update_unsupported_table_raises(self, seeded_engine: Engine) -> None:
        """update_field raises ValueError for unsupported table."""
        paper_id = self._get_paper_id(seeded_engine, "Brown2020GPT3")
        with pytest.raises(ValueError, match="cannot be updated"):
            with with_session(seeded_engine) as session:
                paper_service.update_field(session, paper_id, "nosuch", "col", "v")


class TestDeletePaper:
    """Tests for paper_service.delete_paper."""

    def test_delete_existing_paper(self, clean_engine: Engine) -> None:
        """delete_paper removes a paper and its bib entry."""
        paper = PaperCreate(
            title="Delete Me",
            contents="To be deleted.",
            bibtex_id="DeleteMe2024",
            bibtex="@misc{DeleteMe2024}",
            authors=["Del, Author"],
        )
        with with_session(clean_engine) as session:
            summary = paper_service.add_paper(session, paper)
            paper_id = summary.paper_id

        with with_session(clean_engine) as session:
            paper_service.delete_paper(session, paper_id)

        with with_session(clean_engine) as session:
            assert PaperRepository(session).get_by_bibtex_id("DeleteMe2024") is None

    def test_delete_nonexistent_raises(self, clean_engine: Engine) -> None:
        """delete_paper raises ValueError for a non-existent paper_id."""
        with pytest.raises(ValueError, match="not found"):
            with with_session(clean_engine) as session:
                paper_service.delete_paper(session, 99999)
