"""Integration tests for services/paper_service.py and services/import_service.py.

All tests run against the ephemeral PostgreSQL. No mocking.
"""

import pathlib

import pytest
from sqlalchemy.orm import Session

from paper_sorts.db.repositories import PaperCreate
from paper_sorts.services import import_service, paper_service


class TestPaperService:
    """Tests for paper_service module."""

    def test_search_by_title(self, seeded_session: Session) -> None:
        """search_by_title returns matching papers."""
        results = paper_service.search_by_title(seeded_session, "BERT")
        assert len(results) >= 1
        assert all("BERT" in r.title for r in results)

    def test_search_by_author(self, seeded_session: Session) -> None:
        """search_by_author returns papers for matching author."""
        results = paper_service.search_by_author(seeded_session, "Vaswani")
        assert len(results) >= 1

    def test_add_paper(self, db_session: Session) -> None:
        """add_paper persists and returns a PaperSummary."""
        data = PaperCreate(
            title="Service Test Paper",
            contents="Test abstract.",
            bibtex_id="SvcTest2024",
            bibtex="@article{SvcTest2024}",
            authors=["Service, Test"],
        )
        result = paper_service.add_paper(db_session, data)
        assert result.id is not None
        assert result.title == "Service Test Paper"

    def test_add_paper_duplicate_raises(self, seeded_session: Session) -> None:
        """add_paper raises ValueError when bibtex_id already exists."""
        data = PaperCreate(
            title="Dup",
            contents=".",
            bibtex_id="Vaswani2017Attention",  # already in seed
            bibtex="@article{Vaswani2017Attention}",
            authors=[],
        )
        with pytest.raises(ValueError):
            paper_service.add_paper(seeded_session, data)

    def test_update_field_title(self, seeded_session: Session) -> None:
        """update_field with field='title' updates the title."""
        results = paper_service.search_by_title(seeded_session, "BERT")
        paper_id = results[0].id
        paper_service.update_field(seeded_session, paper_id, "title", "BERT v2")
        seeded_session.flush()
        updated = paper_service.search_by_title(seeded_session, "BERT v2")
        assert any(r.id == paper_id for r in updated)

    def test_update_field_contents(self, seeded_session: Session) -> None:
        """update_field with field='contents' updates the abstract."""
        results = paper_service.search_by_title(seeded_session, "BERT")
        paper_id = results[0].id
        paper_service.update_field(seeded_session, paper_id, "contents", "New abstract")
        seeded_session.flush()
        from paper_sorts.db.repositories import PaperRepository

        repo = PaperRepository(seeded_session)
        p = repo.get_by_id(paper_id)
        assert p is not None
        assert p.contents == "New abstract"

    def test_update_field_author(self, seeded_session: Session) -> None:
        """update_field with field='author' replaces authors."""
        results = paper_service.search_by_title(seeded_session, "BERT")
        paper_id = results[0].id
        paper_service.update_field(seeded_session, paper_id, "author", "New, Author")
        seeded_session.flush()
        from paper_sorts.db.repositories import PaperRepository

        repo = PaperRepository(seeded_session)
        p = repo.get_by_id(paper_id)
        assert p is not None
        assert p.authors == ["New, Author"]

    def test_update_field_not_found_raises(self, db_session: Session) -> None:
        """update_field raises LookupError when paper_id doesn't exist."""
        with pytest.raises(LookupError):
            paper_service.update_field(db_session, 999999, "title", "irrelevant")

    def test_delete_paper(self, seeded_session: Session) -> None:
        """delete_paper removes the paper from the database."""
        results = paper_service.search_by_title(seeded_session, "Large-Scale")
        assert results
        paper_id = results[0].id
        paper_service.delete_paper(seeded_session, paper_id)
        seeded_session.flush()
        gone = paper_service.search_by_title(seeded_session, "Large-Scale")
        assert not any(r.id == paper_id for r in gone)

    def test_delete_paper_not_found_raises(self, db_session: Session) -> None:
        """delete_paper raises LookupError when paper doesn't exist."""
        with pytest.raises(LookupError):
            paper_service.delete_paper(db_session, 999999)


class TestImportService:
    """Tests for import_service module."""

    @pytest.fixture()
    def fixture_dir(self) -> pathlib.Path:
        """Return the path to the tests/fixtures/ directory."""
        return pathlib.Path(__file__).parent / "fixtures"

    def test_extract_papers_from_tex_bib(self, fixture_dir: pathlib.Path) -> None:
        """extract_papers_from_tex_bib yields PaperCreate for matched entries."""
        tex = fixture_dir / "lit_sample.tex"
        bib = fixture_dir / "refs_sample.bib"
        if not tex.exists() or not bib.exists():
            pytest.skip("Fixture files not yet created (T033)")
        results = list(import_service.extract_papers_from_tex_bib(tex, bib))
        # Should yield 2 papers (3rd entry in .tex has no matching .bib record)
        assert len(results) == 2

    def test_missing_bib_key_skipped(self, fixture_dir: pathlib.Path) -> None:
        """Entries with no matching BibTeX key are skipped with a warning."""
        tex = fixture_dir / "lit_sample.tex"
        bib = fixture_dir / "refs_sample.bib"
        if not tex.exists() or not bib.exists():
            pytest.skip("Fixture files not yet created (T033)")
        results = list(import_service.extract_papers_from_tex_bib(tex, bib))
        bib_keys = {r.bibtex_id for r in results}
        # The missing key should NOT be in the results
        assert "MissingKey2024" not in bib_keys
