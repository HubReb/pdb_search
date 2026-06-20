"""Integration tests for the persistence layer (repositories).

All tests run against a real ephemeral PostgreSQL instance provisioned by
pytest-postgresql.  No mocking of the SQLAlchemy session, repositories,
or database driver — see constitution Principle II.

Seed data: tests/fixtures/seed_papers.SEED_PAPERS
"""

import pytest
from sqlalchemy.orm import Session

from paper_sorts.db.repositories import (
    BibRepository,
    PaperCreate,
    PaperRepository,
)
from tests.fixtures.seed_papers import SEED_PAPERS


@pytest.fixture(autouse=True)
def seed(db_session: Session) -> None:
    """Insert all SEED_PAPERS before each test; rollback cleans up.

    :param db_session: Function-scoped session from conftest.
    """
    repo = PaperRepository(db_session)
    for paper in SEED_PAPERS:
        try:
            repo.add(paper)
        except ValueError:
            pass  # Already added (e.g. by a previous fixture call in same session)
    db_session.flush()


# ---------------------------------------------------------------------------
# search_by_title
# ---------------------------------------------------------------------------


class TestSearchByTitle:
    """Tests for PaperRepository.search_by_title."""

    def test_single_match(self, db_session: Session) -> None:
        """search_by_title returns one result for a unique title."""
        repo = PaperRepository(db_session)
        results = repo.search_by_title("Catastrophic Forgetting in Neural Machine Translation")
        assert len(results) == 1
        assert results[0].bibtex_id == "korakakis2022"
        assert "Korakakis, Michalis" in results[0].authors
        assert "Pino, J." in results[0].authors

    def test_multiple_matches(self, db_session: Session) -> None:
        """search_by_title returns multiple results when title is shared."""
        repo = PaperRepository(db_session)
        results = repo.search_by_title("Large-Scale Sentence Alignment with Attention")
        assert len(results) == 2
        bibtex_ids = {r.bibtex_id for r in results}
        assert "Wang2021LargeScaleSA" in bibtex_ids
        assert "Wang2022FollowUp" in bibtex_ids

    def test_no_match(self, db_session: Session) -> None:
        """search_by_title returns empty list when title not found."""
        repo = PaperRepository(db_session)
        results = repo.search_by_title("This paper does not exist")
        assert results == []

    def test_summary_present(self, db_session: Session) -> None:
        """search_by_title includes contents and bibtex in the result."""
        repo = PaperRepository(db_session)
        results = repo.search_by_title("Survey of Low-Resource NMT")
        assert len(results) == 1
        assert results[0].contents != ""
        assert results[0].bibtex != ""


# ---------------------------------------------------------------------------
# search_by_author
# ---------------------------------------------------------------------------


class TestSearchByAuthor:
    """Tests for PaperRepository.search_by_author."""

    def test_found(self, db_session: Session) -> None:
        """search_by_author finds papers by an existing author."""
        repo = PaperRepository(db_session)
        results = repo.search_by_author("Pino, J.")
        # Pino appears in korakakis2022 and pino2020
        assert len(results) >= 2
        bibtex_ids = {r.bibtex_id for r in results}
        assert "korakakis2022" in bibtex_ids
        assert "pino2020" in bibtex_ids

    def test_not_found(self, db_session: Session) -> None:
        """search_by_author returns empty list for an unknown author."""
        repo = PaperRepository(db_session)
        results = repo.search_by_author("Nobody, Unknown")
        assert results == []

    def test_single_author_paper(self, db_session: Session) -> None:
        """search_by_author returns the correct paper for a sole author."""
        repo = PaperRepository(db_session)
        results = repo.search_by_author("Smith, Alice")
        assert len(results) == 1
        assert results[0].bibtex_id == "survey2023"


# ---------------------------------------------------------------------------
# add_paper
# ---------------------------------------------------------------------------


class TestAddPaper:
    """Tests for PaperRepository.add."""

    def test_add_persists(self, db_session: Session) -> None:
        """Adding a paper makes it retrievable by title and author."""
        repo = PaperRepository(db_session)
        new_paper = PaperCreate(
            title="A Brand New Paper",
            contents="Summary of new paper.",
            bibtex_id="new2026",
            authors=["Test, Author"],
            bibtex="@article{new2026, author={Test, Author}, title={A Brand New Paper}}",
        )
        repo.add(new_paper)
        db_session.flush()

        results = repo.search_by_title("A Brand New Paper")
        assert len(results) == 1
        assert results[0].bibtex_id == "new2026"

    def test_duplicate_bibtex_id_rejected(self, db_session: Session) -> None:
        """Adding a paper with an existing bibtex_id raises ValueError."""
        repo = PaperRepository(db_session)
        with pytest.raises(ValueError, match="already exists"):
            repo.add(SEED_PAPERS[0])


# ---------------------------------------------------------------------------
# delete_paper
# ---------------------------------------------------------------------------


class TestDeletePaper:
    """Tests for PaperRepository.delete."""

    def test_delete_removes_paper(self, db_session: Session) -> None:
        """Deleting a paper removes it from search results."""
        repo = PaperRepository(db_session)
        results = repo.search_by_title("Survey of Low-Resource NMT")
        assert len(results) == 1
        paper_id = results[0].id

        repo.delete(paper_id)
        db_session.flush()

        after = repo.search_by_title("Survey of Low-Resource NMT")
        assert after == []

    def test_delete_nonexistent_raises(self, db_session: Session) -> None:
        """Deleting a non-existent paper ID raises ValueError."""
        repo = PaperRepository(db_session)
        with pytest.raises(ValueError, match="not found"):
            repo.delete(999999)

    def test_delete_removes_orphan_author(self, db_session: Session) -> None:
        """Deleting the only paper by an author removes the author too."""
        from sqlalchemy import select

        from paper_sorts.db.models import Author

        repo = PaperRepository(db_session)
        results = repo.search_by_title("Survey of Low-Resource NMT")
        assert results
        paper_id = results[0].id
        repo.delete(paper_id)
        db_session.flush()

        # Smith, Alice should be gone
        smith = db_session.scalar(select(Author).where(Author.author == "Smith, Alice"))
        assert smith is None


# ---------------------------------------------------------------------------
# update_field
# ---------------------------------------------------------------------------


class TestUpdateField:
    """Tests for PaperRepository.update_field."""

    def test_update_title(self, db_session: Session) -> None:
        """update_field updates papers.title."""
        repo = PaperRepository(db_session)
        results = repo.search_by_title("Survey of Low-Resource NMT")
        paper_id = results[0].id

        repo.update_field("papers", "title", str(paper_id), "Updated Title")
        db_session.flush()

        updated = repo.search_by_title("Updated Title")
        assert len(updated) == 1

    def test_update_contents(self, db_session: Session) -> None:
        """update_field updates papers.contents."""
        repo = PaperRepository(db_session)
        results = repo.search_by_title("Survey of Low-Resource NMT")
        paper_id = results[0].id

        repo.update_field("papers", "contents", str(paper_id), "New summary.")
        db_session.flush()

        updated = repo.search_by_title("Survey of Low-Resource NMT")
        assert updated[0].contents == "New summary."

    def test_update_bibtex(self, db_session: Session) -> None:
        """update_field updates bib.bibtex."""
        repo = PaperRepository(db_session)
        new_bib = "@article{survey2023, author={Smith, Alice}, title={Updated}}"
        repo.update_field("bib", "bibtex", "survey2023", new_bib)
        db_session.flush()

        bib_repo = BibRepository(db_session)
        assert bib_repo.get("survey2023") == new_bib

    def test_update_unsupported_table_raises(self, db_session: Session) -> None:
        """update_field raises ValueError for an unsupported table."""
        repo = PaperRepository(db_session)
        with pytest.raises(ValueError, match="Unsupported table"):
            repo.update_field("authors_papers", "paper_id", "1", "2")

    def test_update_unsupported_column_raises(self, db_session: Session) -> None:
        """update_field raises ValueError for a non-editable column."""
        repo = PaperRepository(db_session)
        results = repo.search_by_title("Survey of Low-Resource NMT")
        paper_id = results[0].id
        with pytest.raises(ValueError):
            repo.update_field("papers", "bibtex_id", str(paper_id), "new_key")
