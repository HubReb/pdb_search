"""Integration tests for the persistence layer (repositories).

All tests run against the ephemeral PostgreSQL cluster provided by the
``seeded_session`` fixture.  No mocking of the SQLAlchemy session,
repositories, or driver (constitution Principle II).

Seed data is defined in ``tests/fixtures/seed_papers.py``.
"""

from __future__ import annotations

import pytest
from sqlalchemy.orm import Session

from paper_sorts.db.repositories import (
    AuthorRepository,
    BibRepository,
    PaperCreate,
    PaperRepository,
    PaperSummary,
)

# ---------------------------------------------------------------------------
# BibRepository tests
# ---------------------------------------------------------------------------


class TestBibRepository:
    """Tests for :class:`BibRepository`."""

    def test_get_by_key_existing(self, seeded_session: Session) -> None:
        """get_by_key returns a Bib row for a key that exists."""
        bib = BibRepository.get_by_key(seeded_session, "Lee2022DirectSpeech")
        assert bib is not None
        assert bib.bibtex_id == "Lee2022DirectSpeech"

    def test_get_by_key_missing(self, seeded_session: Session) -> None:
        """get_by_key returns None for a key that does not exist."""
        bib = BibRepository.get_by_key(seeded_session, "DoesNotExist999")
        assert bib is None

    def test_create(self, db_session: Session) -> None:
        """create inserts a new Bib row."""
        bib = BibRepository.create(db_session, "NewKey2026", "@article{NewKey2026}")
        assert bib.bibtex_id == "NewKey2026"
        assert bib.bibtex == "@article{NewKey2026}"


# ---------------------------------------------------------------------------
# AuthorRepository tests
# ---------------------------------------------------------------------------


class TestAuthorRepository:
    """Tests for :class:`AuthorRepository`."""

    def test_get_or_create_new(self, db_session: Session) -> None:
        """get_or_create creates a new Author when the name doesn't exist."""
        author = AuthorRepository.get_or_create(db_session, "Doe, John")
        assert author.author == "Doe, John"
        assert author.id is not None

    def test_get_or_create_existing(self, db_session: Session) -> None:
        """get_or_create returns the same Author on repeated calls."""
        first = AuthorRepository.get_or_create(db_session, "Smith, Jane")
        second = AuthorRepository.get_or_create(db_session, "Smith, Jane")
        assert first.id == second.id

    def test_get_by_paper_id(self, seeded_session: Session) -> None:
        """get_by_paper_id returns all authors linked to a paper."""
        # Find Lee2022DirectSpeech
        results = PaperRepository.search_by_title(
            seeded_session, "Direct speech-to-speech translation with discrete units"
        )
        assert len(results) == 1
        paper = results[0]
        authors = AuthorRepository.get_by_paper_id(seeded_session, paper.paper_id)
        author_names = [a.author for a in authors]
        assert "Lee, Ann" in author_names
        assert "Chen, Peng-Jen" in author_names


# ---------------------------------------------------------------------------
# PaperRepository tests
# ---------------------------------------------------------------------------


class TestPaperRepository:
    """Tests for :class:`PaperRepository`."""

    def test_search_by_title_unique(self, seeded_session: Session) -> None:
        """search_by_title returns exactly one result for a unique title."""
        results = PaperRepository.search_by_title(
            seeded_session, "Large-scale Self- and Semi-Supervised learning for speech translation"
        )
        assert len(results) == 1
        assert results[0].bibtex_key == "Wang2021LargeScaleSA"

    def test_search_by_title_empty(self, seeded_session: Session) -> None:
        """search_by_title returns an empty list when nothing matches."""
        results = PaperRepository.search_by_title(seeded_session, "no such title anywhere")
        assert results == []

    def test_search_by_title_multiple(self, seeded_session: Session) -> None:
        """search_by_title with a prefix only returns exact matches (not LIKE)."""
        # "Direct speech-to-speech..." is different from "Direct speech translation..."
        results1 = PaperRepository.search_by_title(
            seeded_session, "Direct speech-to-speech translation with discrete units"
        )
        results2 = PaperRepository.search_by_title(
            seeded_session, "Direct speech translation for low-resource languages"
        )
        assert len(results1) == 1
        assert len(results2) == 1
        assert results1[0].bibtex_key != results2[0].bibtex_key

    def test_search_by_author(self, seeded_session: Session) -> None:
        """search_by_author returns all papers by a given author."""
        results = PaperRepository.search_by_author(seeded_session, "Lee, Ann")
        keys = {r.bibtex_key for r in results}
        # Lee, Ann is in Paper 1 and Paper 3
        assert "Lee2022DirectSpeech" in keys
        assert "Wang2021LargeScaleSA" in keys

    def test_search_by_author_not_found(self, seeded_session: Session) -> None:
        """search_by_author returns empty list when author is not in DB."""
        results = PaperRepository.search_by_author(seeded_session, "Nobody, X.")
        assert results == []

    def test_get_by_id(self, seeded_session: Session) -> None:
        """get_by_id returns a PaperSummary for a valid paper id."""
        results = PaperRepository.search_by_title(
            seeded_session, "Direct speech-to-speech translation with discrete units"
        )
        paper_id = results[0].paper_id
        summary = PaperRepository.get_by_id(seeded_session, paper_id)
        assert summary is not None
        assert summary.bibtex_key == "Lee2022DirectSpeech"

    def test_get_by_id_not_found(self, seeded_session: Session) -> None:
        """get_by_id returns None for a non-existent paper id."""
        result = PaperRepository.get_by_id(seeded_session, 999999)
        assert result is None

    def test_create(self, db_session: Session) -> None:
        """create inserts paper, bib entry, and author links."""
        data = PaperCreate(
            title="Test Paper for Create",
            authors=["Test, Author"],
            bibtex_key="TestCreate2026",
            summary="A test paper.",
            bibtex_text="@article{TestCreate2026}",
        )
        summary = PaperRepository.create(db_session, data)
        assert isinstance(summary, PaperSummary)
        assert summary.title == "Test Paper for Create"
        assert "Test, Author" in summary.authors

    def test_update_title(self, db_session: Session) -> None:
        """update_field can update the title of a paper."""
        data = PaperCreate(
            title="Original Title",
            authors=["Update, Author"],
            bibtex_key="TestUpdate2026Title",
            summary="Test summary.",
            bibtex_text="@article{TestUpdate2026Title}",
        )
        summary = PaperRepository.create(db_session, data)
        paper_id = summary.paper_id
        PaperRepository.update_field(db_session, paper_id, "title", "Updated Title")
        updated = PaperRepository.get_by_id(db_session, paper_id)
        assert updated is not None
        assert updated.title == "Updated Title"

    def test_update_contents(self, db_session: Session) -> None:
        """update_field can update the summary (contents) of a paper."""
        data = PaperCreate(
            title="Title for Contents Update",
            authors=["Contents, Author"],
            bibtex_key="TestUpdate2026Contents",
            summary="Original summary.",
            bibtex_text="@article{TestUpdate2026Contents}",
        )
        summary = PaperRepository.create(db_session, data)
        PaperRepository.update_field(db_session, summary.paper_id, "contents", "New summary.")
        updated = PaperRepository.get_by_id(db_session, summary.paper_id)
        assert updated is not None
        assert updated.summary == "New summary."

    def test_update_bibtex(self, db_session: Session) -> None:
        """update_field can update the bibtex text of a paper."""
        data = PaperCreate(
            title="Title for Bibtex Update",
            authors=["Bib, Author"],
            bibtex_key="TestUpdate2026Bib",
            summary="Summary.",
            bibtex_text="@article{TestUpdate2026Bib, year={2026}}",
        )
        summary = PaperRepository.create(db_session, data)
        new_bib = "@article{TestUpdate2026Bib, year={2027}}"
        PaperRepository.update_field(db_session, summary.paper_id, "bibtex", new_bib)
        updated = PaperRepository.get_by_id(db_session, summary.paper_id)
        assert updated is not None
        assert updated.bibtex_text == new_bib

    def test_update_author(self, db_session: Session) -> None:
        """update_field replaces author links with a single new author."""
        data = PaperCreate(
            title="Title for Author Update",
            authors=["Old, Author", "Another, Author"],
            bibtex_key="TestUpdate2026Author",
            summary="Summary.",
            bibtex_text="@article{TestUpdate2026Author}",
        )
        summary = PaperRepository.create(db_session, data)
        PaperRepository.update_field(db_session, summary.paper_id, "author", "New, Author")
        updated = PaperRepository.get_by_id(db_session, summary.paper_id)
        assert updated is not None
        assert updated.authors == ["New, Author"]

    def test_update_unknown_field(self, db_session: Session) -> None:
        """update_field raises ValueError for unknown field name."""
        data = PaperCreate(
            title="Title for Unknown Field",
            authors=["Field, Author"],
            bibtex_key="TestUpdate2026Field",
            summary="Summary.",
            bibtex_text="@article{TestUpdate2026Field}",
        )
        summary = PaperRepository.create(db_session, data)
        with pytest.raises(ValueError, match="Unknown field"):
            PaperRepository.update_field(db_session, summary.paper_id, "bogus", "value")

    def test_delete(self, db_session: Session) -> None:
        """delete removes the paper and its author links."""
        data = PaperCreate(
            title="Paper to Delete",
            authors=["Delete, Author"],
            bibtex_key="TestDelete2026",
            summary="Will be deleted.",
            bibtex_text="@article{TestDelete2026}",
        )
        summary = PaperRepository.create(db_session, data)
        paper_id = summary.paper_id
        PaperRepository.delete(db_session, paper_id)
        assert PaperRepository.get_by_id(db_session, paper_id) is None

    def test_delete_not_found(self, db_session: Session) -> None:
        """delete raises ValueError for non-existent paper id."""
        with pytest.raises(ValueError, match="not found"):
            PaperRepository.delete(db_session, 999999)
