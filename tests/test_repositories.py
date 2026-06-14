"""Integration tests for PaperRepository against a real PostgreSQL database.

Per the project constitution Principle II: no mocking of the SQLAlchemy
session, repositories, or database driver.  Tests run against an ephemeral
PostgreSQL provisioned by pytest-postgresql.

Seed data: tests/fixtures/seed_papers.py::SEED_PAPERS
"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import IntegrityError

from paper_sorts.db.repositories import PaperCreate, PaperRepository, PaperSummary
from paper_sorts.db.session import with_session

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def repo() -> PaperRepository:
    """Return a fresh PaperRepository instance."""
    return PaperRepository()


# ---------------------------------------------------------------------------
# Search tests
# ---------------------------------------------------------------------------


class TestSearchByTitle:
    """Tests for PaperRepository.search_by_title."""

    def test_found_single_match(self, seeded_engine: object, repo: PaperRepository) -> None:
        """Search by title returns one PaperSummary when exactly one paper matches."""
        # SEED_PAPERS[0].title = "Direct speech-to-speech translation with discrete units"
        with with_session(seeded_engine) as session:  # type: ignore[arg-type]
            results = repo.search_by_title(
                session, "Direct speech-to-speech translation with discrete units"
            )
        assert len(results) == 1
        paper = results[0]
        assert paper.title == "Direct speech-to-speech translation with discrete units"
        assert "Lee, Ann" in paper.authors
        assert "Chen, Peng-Jen" in paper.authors
        assert "Lee2022DirectSpeech" == paper.bibtex_id

    def test_not_found(self, seeded_engine: object, repo: PaperRepository) -> None:
        """Search by title returns empty list for a title not in the database."""
        with with_session(seeded_engine) as session:  # type: ignore[arg-type]
            results = repo.search_by_title(session, "no such title")
        assert results == []

    def test_multiple_papers_same_author_different_titles(
        self, seeded_engine: object, repo: PaperRepository
    ) -> None:
        """Two papers with the same author but different titles are each findable."""
        with with_session(seeded_engine) as session:  # type: ignore[arg-type]
            r1 = repo.search_by_title(session, "Shared title paper variant A")
            r2 = repo.search_by_title(session, "Shared title paper variant B")
        assert len(r1) == 1
        assert len(r2) == 1
        assert r1[0].bibtex_id == "SharedA2023"
        assert r2[0].bibtex_id == "SharedB2023"


class TestSearchByAuthor:
    """Tests for PaperRepository.search_by_author."""

    def test_found(self, seeded_engine: object, repo: PaperRepository) -> None:
        """Searching by a known author returns all papers they authored."""
        # SEED_PAPERS[0] and [1] both have "Wang, Changhan"
        with with_session(seeded_engine) as session:  # type: ignore[arg-type]
            results = repo.search_by_author(session, "Wang, Changhan")
        assert len(results) == 2
        titles = {p.title for p in results}
        assert "Direct speech-to-speech translation with discrete units" in titles
        assert "Large-scale Self- and Semi-Supervised learning for speech translation" in titles

    def test_not_found(self, seeded_engine: object, repo: PaperRepository) -> None:
        """Searching by an unknown author returns an empty list."""
        with with_session(seeded_engine) as session:  # type: ignore[arg-type]
            results = repo.search_by_author(session, "No Such Author")
        assert results == []

    def test_result_has_bibtex(self, seeded_engine: object, repo: PaperRepository) -> None:
        """Each search result includes the bibtex content."""
        with with_session(seeded_engine) as session:  # type: ignore[arg-type]
            results = repo.search_by_author(session, "Pino, J.")
        assert len(results) >= 1
        assert results[0].bibtex != ""


# ---------------------------------------------------------------------------
# Add tests
# ---------------------------------------------------------------------------


class TestAddPaper:
    """Tests for PaperRepository.add_paper."""

    def test_add_and_retrieve(self, engine: object, repo: PaperRepository) -> None:
        """Adding a paper makes it retrievable by title."""
        paper = PaperCreate(
            title="Test Add Paper",
            contents="A test summary.",
            bibtex_id="TestAdd2024",
            bibtex="@article{TestAdd2024, title={Test Add Paper}, year={2024}}",
            authors=["Tester, T."],
        )
        with with_session(engine) as session:  # type: ignore[arg-type]
            result = repo.add_paper(session, paper)

        assert result.title == "Test Add Paper"
        assert result.bibtex_id == "TestAdd2024"
        assert "Tester, T." in result.authors

        # Cleanup
        with with_session(engine) as session:  # type: ignore[arg-type]
            results = repo.search_by_title(session, "Test Add Paper")
            assert len(results) == 1
            repo.delete_paper(session, results[0].id)

    def test_duplicate_bibtex_id_raises(self, engine: object, repo: PaperRepository) -> None:
        """Adding a paper with an existing bibtex_id raises IntegrityError."""
        paper = PaperCreate(
            title="Dup Paper",
            contents="Dup summary.",
            bibtex_id="DupKey2024",
            bibtex="@article{DupKey2024, title={Dup}, year={2024}}",
            authors=["Author, A."],
        )
        with with_session(engine) as session:  # type: ignore[arg-type]
            repo.add_paper(session, paper)

        with pytest.raises(IntegrityError):
            with with_session(engine) as session:  # type: ignore[arg-type]
                duplicate = PaperCreate(
                    title="Different Title",
                    contents="Different summary.",
                    bibtex_id="DupKey2024",  # same key!
                    bibtex="@article{DupKey2024, title={Different}, year={2025}}",
                    authors=["Author, B."],
                )
                repo.add_paper(session, duplicate)

        # Cleanup
        with with_session(engine) as session:  # type: ignore[arg-type]
            results = repo.search_by_title(session, "Dup Paper")
            if results:
                repo.delete_paper(session, results[0].id)


# ---------------------------------------------------------------------------
# Update tests
# ---------------------------------------------------------------------------


class TestUpdateField:
    """Tests for PaperRepository.update_field."""

    def _add_paper(self, engine: object, repo: PaperRepository) -> PaperSummary:
        """Helper: add a paper and return its summary."""
        with with_session(engine) as session:  # type: ignore[arg-type]
            return repo.add_paper(
                session,
                PaperCreate(
                    title="Update Test Paper",
                    contents="Original contents.",
                    bibtex_id="UpdateTest2024",
                    bibtex="@article{UpdateTest2024, title={Update Test}, year={2024}}",
                    authors=["Author, U."],
                ),
            )

    def test_update_title(self, engine: object, repo: PaperRepository) -> None:
        """Updating 'title' in 'papers' persists the new value."""
        paper = self._add_paper(engine, repo)
        try:
            with with_session(engine) as session:  # type: ignore[arg-type]
                repo.update_field(session, paper.id, "papers", "title", "New Title")
            with with_session(engine) as session:  # type: ignore[arg-type]
                results = repo.search_by_title(session, "New Title")
            assert len(results) == 1
        finally:
            with with_session(engine) as session:  # type: ignore[arg-type]
                results = repo.search_by_title(session, "New Title")
                if results:
                    repo.delete_paper(session, results[0].id)

    def test_update_contents(self, engine: object, repo: PaperRepository) -> None:
        """Updating 'contents' in 'papers' persists the new value."""
        paper = self._add_paper(engine, repo)
        try:
            with with_session(engine) as session:  # type: ignore[arg-type]
                repo.update_field(session, paper.id, "papers", "contents", "Updated summary.")
            with with_session(engine) as session:  # type: ignore[arg-type]
                results = repo.search_by_title(session, "Update Test Paper")
            assert results[0].contents == "Updated summary."
        finally:
            with with_session(engine) as session:  # type: ignore[arg-type]
                results = repo.search_by_title(session, "Update Test Paper")
                if results:
                    repo.delete_paper(session, results[0].id)

    def test_update_bibtex(self, engine: object, repo: PaperRepository) -> None:
        """Updating 'bibtex' in 'bib' persists the new value."""
        paper = self._add_paper(engine, repo)
        new_bib = "@article{UpdateTest2024, title={Updated}, year={2025}}"
        try:
            with with_session(engine) as session:  # type: ignore[arg-type]
                repo.update_field(session, paper.id, "bib", "bibtex", new_bib)
            with with_session(engine) as session:  # type: ignore[arg-type]
                results = repo.search_by_title(session, "Update Test Paper")
            assert results[0].bibtex == new_bib
        finally:
            with with_session(engine) as session:  # type: ignore[arg-type]
                results = repo.search_by_title(session, "Update Test Paper")
                if results:
                    repo.delete_paper(session, results[0].id)

    def test_update_invalid_field_raises(self, engine: object, repo: PaperRepository) -> None:
        """Updating an unsupported field raises ValueError."""
        paper = self._add_paper(engine, repo)
        try:
            with pytest.raises(ValueError, match="not updatable"):
                with with_session(engine) as session:  # type: ignore[arg-type]
                    repo.update_field(session, paper.id, "papers", "id", "999")
        finally:
            with with_session(engine) as session:  # type: ignore[arg-type]
                results = repo.search_by_title(session, "Update Test Paper")
                if results:
                    repo.delete_paper(session, results[0].id)


# ---------------------------------------------------------------------------
# Delete tests
# ---------------------------------------------------------------------------


class TestDeletePaper:
    """Tests for PaperRepository.delete_paper."""

    def test_delete_removes_paper(self, engine: object, repo: PaperRepository) -> None:
        """Deleting a paper makes it unretrievable by title."""
        with with_session(engine) as session:  # type: ignore[arg-type]
            paper = repo.add_paper(
                session,
                PaperCreate(
                    title="To Be Deleted",
                    contents="Delete test.",
                    bibtex_id="DelTest2024",
                    bibtex="@article{DelTest2024, title={Delete}, year={2024}}",
                    authors=["Author, D."],
                ),
            )
        with with_session(engine) as session:  # type: ignore[arg-type]
            repo.delete_paper(session, paper.id)
        with with_session(engine) as session:  # type: ignore[arg-type]
            results = repo.search_by_title(session, "To Be Deleted")
        assert results == []

    def test_delete_nonexistent_raises(self, engine: object, repo: PaperRepository) -> None:
        """Deleting a paper that does not exist raises ValueError."""
        with pytest.raises(ValueError, match="not found"):
            with with_session(engine) as session:  # type: ignore[arg-type]
                repo.delete_paper(session, 999_999)
