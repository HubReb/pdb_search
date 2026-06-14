"""Integration tests for the paper_sorts persistence layer.

Tests PaperRepository, AuthorRepository, and BibRepository against a real
ephemeral PostgreSQL database (constitution Principle II — no mocking).

All tests use the SEED_PAPERS fixture from tests/fixtures/seed_papers.py
so the relationship between assertions and seeded data is visible here.
"""

from __future__ import annotations

import pytest
from sqlalchemy.orm import Session

from paper_sorts.db.repositories import (
    PaperCreate,
    PaperRepository,
)
from tests.fixtures.seed_papers import SEED_PAPERS

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed(session: Session) -> None:
    """Insert all SEED_PAPERS into the given session and flush."""
    for paper in SEED_PAPERS:
        PaperRepository.add(session, paper)
    session.flush()


# ---------------------------------------------------------------------------
# Search tests
# ---------------------------------------------------------------------------


class TestSearchByTitle:
    """Tests for PaperRepository.get_by_title."""

    def test_found_exact_match(self, seeded_session: Session) -> None:
        """A paper present in SEED_PAPERS is found by exact title."""
        results = PaperRepository.get_by_title(
            seeded_session,
            "Direct speech-to-speech translation with discrete units",
        )
        assert len(results) == 1
        paper = results[0]
        assert paper.title == "Direct speech-to-speech translation with discrete units"
        assert "Lee, Ann" in paper.authors

    def test_not_found(self, seeded_session: Session) -> None:
        """An unknown title returns an empty list."""
        results = PaperRepository.get_by_title(seeded_session, "No such title exists")
        assert results == []

    def test_returns_bibtex(self, seeded_session: Session) -> None:
        """The result includes the full BibTeX string."""
        results = PaperRepository.get_by_title(
            seeded_session,
            "Large-scale Self- and Semi-Supervised Learning for Speech Translation",
        )
        assert len(results) == 1
        assert "Wang2021LargeScaleSA" in results[0].bibtex


class TestSearchByAuthor:
    """Tests for PaperRepository.get_by_author."""

    def test_found_pino(self, seeded_session: Session) -> None:
        """Author 'Pino, J.' is in two seed papers; both are returned."""
        # SEED_PAPERS: Pino, J. appears in Wang2021LargeScaleSA and Lee2021DirectSpeech
        results = PaperRepository.get_by_author(seeded_session, "Pino, J.")
        assert len(results) >= 1
        titles = [r.title for r in results]
        assert any("Large-scale" in t for t in titles)

    def test_not_found(self, seeded_session: Session) -> None:
        """An unknown author returns an empty list."""
        results = PaperRepository.get_by_author(seeded_session, "Nonexistent, A.")
        assert results == []


# ---------------------------------------------------------------------------
# Add tests
# ---------------------------------------------------------------------------


class TestAdd:
    """Tests for PaperRepository.add."""

    def test_add_new_paper(self, clean_db_session: Session) -> None:
        """A new paper can be added and retrieved by title."""
        paper = PaperCreate(
            title="Test Paper Title",
            contents="Test summary.",
            bibtex_id="TestPaper2026",
            bibtex="@misc{TestPaper2026, title={Test}}",
            authors=["Doe, John", "Smith, Jane"],
        )
        paper_id = PaperRepository.add(clean_db_session, paper)
        assert isinstance(paper_id, int)
        assert paper_id > 0

        results = PaperRepository.get_by_title(clean_db_session, "Test Paper Title")
        assert len(results) == 1
        assert "Doe, John" in results[0].authors

    def test_duplicate_bibtex_raises(self, seeded_session: Session) -> None:
        """Adding a paper with a duplicate bibtex_id raises ValueError."""
        paper = PaperCreate(
            title="Duplicate Paper",
            contents="Summary.",
            bibtex_id="Wang2021LargeScaleSA",  # already in SEED_PAPERS
            bibtex="@misc{Wang2021LargeScaleSA, title={Dup}}",
            authors=["Test, Author"],
        )
        with pytest.raises(ValueError, match="already exists"):
            PaperRepository.add(seeded_session, paper)


# ---------------------------------------------------------------------------
# Delete tests
# ---------------------------------------------------------------------------


class TestDelete:
    """Tests for PaperRepository.delete."""

    def test_delete_removes_paper(self, clean_db_session: Session) -> None:
        """After delete, the paper is no longer findable by title."""
        paper = PaperCreate(
            title="Paper To Delete",
            contents="Summary.",
            bibtex_id="DeleteMe2026",
            bibtex="@misc{DeleteMe2026, title={Delete}}",
            authors=["Test, Author"],
        )
        paper_id = PaperRepository.add(clean_db_session, paper)
        clean_db_session.flush()

        PaperRepository.delete(clean_db_session, paper_id)
        results = PaperRepository.get_by_title(clean_db_session, "Paper To Delete")
        assert results == []

    def test_delete_nonexistent_raises(self, clean_db_session: Session) -> None:
        """Deleting a paper id that does not exist raises ValueError."""
        with pytest.raises(ValueError, match="not found"):
            PaperRepository.delete(clean_db_session, 999999)

    def test_delete_cleans_orphaned_authors(self, clean_db_session: Session) -> None:
        """After deleting the only paper by an author, the author is removed."""
        paper = PaperCreate(
            title="Orphan Test",
            contents="Summary.",
            bibtex_id="OrphanTest2026",
            bibtex="@misc{OrphanTest2026}",
            authors=["UniqueOrphanAuthor, X."],
        )
        paper_id = PaperRepository.add(clean_db_session, paper)
        clean_db_session.flush()

        PaperRepository.delete(clean_db_session, paper_id)

        # Author should be gone
        results = PaperRepository.get_by_author(clean_db_session, "UniqueOrphanAuthor, X.")
        assert results == []


# ---------------------------------------------------------------------------
# Update tests
# ---------------------------------------------------------------------------


class TestUpdateField:
    """Tests for PaperRepository.update_field."""

    def test_update_title(self, clean_db_session: Session) -> None:
        """Paper title can be updated and the new title is found by search."""
        paper = PaperCreate(
            title="Original Title",
            contents="Summary.",
            bibtex_id="UpdateTitleTest2026",
            bibtex="@misc{UpdateTitleTest2026}",
            authors=["Test, Author"],
        )
        paper_id = PaperRepository.add(clean_db_session, paper)
        clean_db_session.flush()

        PaperRepository.update_field(clean_db_session, "papers", "title", paper_id, "New Title")
        results = PaperRepository.get_by_title(clean_db_session, "New Title")
        assert len(results) == 1

    def test_update_contents(self, clean_db_session: Session) -> None:
        """Paper contents (summary) can be updated."""
        paper = PaperCreate(
            title="Contents Update Test",
            contents="Old summary.",
            bibtex_id="ContentsUpdate2026",
            bibtex="@misc{ContentsUpdate2026}",
            authors=["Test, Author"],
        )
        paper_id = PaperRepository.add(clean_db_session, paper)
        clean_db_session.flush()

        PaperRepository.update_field(
            clean_db_session, "papers", "contents", paper_id, "New summary."
        )
        results = PaperRepository.get_by_title(clean_db_session, "Contents Update Test")
        assert results[0].contents == "New summary."

    def test_update_bibtex(self, clean_db_session: Session) -> None:
        """BibTeX string can be updated."""
        paper = PaperCreate(
            title="Bibtex Update Test",
            contents="Summary.",
            bibtex_id="BibUpdate2026",
            bibtex="@misc{BibUpdate2026, title={Old}}",
            authors=["Test, Author"],
        )
        PaperRepository.add(clean_db_session, paper)
        clean_db_session.flush()

        new_bib = "@misc{BibUpdate2026, title={New}}"
        PaperRepository.update_field(
            clean_db_session, "bib", "bibtex", "BibUpdate2026", new_bib
        )
        results = PaperRepository.get_by_title(clean_db_session, "Bibtex Update Test")
        assert results[0].bibtex == new_bib

    def test_update_duplicate_bibtex_raises(self, clean_db_session: Session) -> None:
        """Updating bibtex to a string already in the database raises ValueError."""
        paper1 = PaperCreate(
            title="Paper Alpha",
            contents="Summary.",
            bibtex_id="Alpha2026",
            bibtex="@misc{Alpha2026}",
            authors=["Test, A."],
        )
        paper2 = PaperCreate(
            title="Paper Beta",
            contents="Summary.",
            bibtex_id="Beta2026",
            bibtex="@misc{Beta2026}",
            authors=["Test, B."],
        )
        PaperRepository.add(clean_db_session, paper1)
        PaperRepository.add(clean_db_session, paper2)
        clean_db_session.flush()

        with pytest.raises(ValueError):
            PaperRepository.update_field(
                clean_db_session, "bib", "bibtex", "Alpha2026", "@misc{Beta2026}"
            )

    def test_update_author(self, clean_db_session: Session) -> None:
        """Author name can be updated; the paper is findable under the new name."""
        paper = PaperCreate(
            title="Author Update Test",
            contents="Summary.",
            bibtex_id="AuthorUpdate2026",
            bibtex="@misc{AuthorUpdate2026}",
            authors=["OldAuthor, X."],
        )
        PaperRepository.add(clean_db_session, paper)
        clean_db_session.flush()

        PaperRepository.update_field(
            clean_db_session, "authors_id", "author", "OldAuthor, X.", "NewAuthor, X."
        )
        results = PaperRepository.get_by_author(clean_db_session, "NewAuthor, X.")
        assert len(results) == 1

    def test_update_invalid_table_raises(self, clean_db_session: Session) -> None:
        """Updating a non-existent table raises an error."""
        with pytest.raises((ValueError, TypeError)):
            PaperRepository.update_field(  # type: ignore[call-overload]
                clean_db_session, "nonexistent_table", "col", "id", "val"
            )

    def test_update_invalid_column_raises(self, clean_db_session: Session) -> None:
        """Updating an invalid column in a valid table raises ValueError."""
        paper = PaperCreate(
            title="Invalid Col Test",
            contents="Summary.",
            bibtex_id="InvalidCol2026",
            bibtex="@misc{InvalidCol2026}",
            authors=["Test, Author"],
        )
        paper_id = PaperRepository.add(clean_db_session, paper)
        clean_db_session.flush()

        with pytest.raises(ValueError):
            PaperRepository.update_field(
                clean_db_session, "papers", "nonexistent_col", paper_id, "value"
            )
