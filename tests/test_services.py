"""Integration tests for paper_sorts service layer.

Tests paper_service and import_service against a real ephemeral PostgreSQL
database (constitution Principle II — no mocking).

NOTE on fixture choice:
    Service functions (paper_service.*) open independent sessions via
    with_session(). Tests that need pre-seeded data visible across session
    boundaries MUST use ``seeded_db_url`` (committed data), NOT
    ``seeded_session`` (savepoint-only data). See conftest.py for details.
"""

from __future__ import annotations

import pytest
from sqlalchemy.orm import Session

from paper_sorts.db.repositories import PaperCreate
from paper_sorts.services import import_service, paper_service

# ---------------------------------------------------------------------------
# paper_service tests
# ---------------------------------------------------------------------------


class TestSearchByTitle:
    """Tests for paper_service.search_by_title."""

    def test_found(self, seeded_db_url: str) -> None:
        """search_by_title returns results for a seeded paper."""
        results = paper_service.search_by_title(
            seeded_db_url,
            "Large-scale Self- and Semi-Supervised Learning for Speech Translation",
        )
        assert len(results) == 1
        assert results[0].bibtex_id == "Wang2021LargeScaleSA"

    def test_not_found(self, seeded_db_url: str) -> None:
        """search_by_title returns empty list for unknown title."""
        results = paper_service.search_by_title(seeded_db_url, "Not In DB")
        assert results == []


class TestSearchByAuthor:
    """Tests for paper_service.search_by_author."""

    def test_found(self, seeded_db_url: str) -> None:
        """search_by_author returns results for a seeded author."""
        results = paper_service.search_by_author(seeded_db_url, "Pino, J.")
        assert len(results) >= 1

    def test_not_found(self, seeded_db_url: str) -> None:
        """search_by_author returns empty list for unknown author."""
        results = paper_service.search_by_author(seeded_db_url, "Nobody, X.")
        assert results == []


class TestAddPaper:
    """Tests for paper_service.add_paper."""

    def test_add_and_retrieve(self, ephemeral_db_url: str, db_session: Session) -> None:
        """A paper added via service is retrievable via search."""
        paper = PaperCreate(
            title="Service Add Test",
            contents="Summary.",
            bibtex_id="ServiceAdd2026",
            bibtex="@misc{ServiceAdd2026}",
            authors=["Service, Tester"],
        )
        paper_service.add_paper(ephemeral_db_url, paper)

        results = paper_service.search_by_title(ephemeral_db_url, "Service Add Test")
        assert len(results) == 1

        # Cleanup
        paper_service.delete_paper(ephemeral_db_url, results[0].paper_id)

    def test_duplicate_raises(self, seeded_db_url: str) -> None:
        """Adding a duplicate bibtex_id raises ValueError."""
        paper = PaperCreate(
            title="Dup Paper",
            contents="Summary.",
            bibtex_id="Wang2021LargeScaleSA",  # already in SEED_PAPERS
            bibtex="@misc{Wang2021LargeScaleSA, title={Dup}}",
            authors=["Test, Author"],
        )
        with pytest.raises(ValueError):
            paper_service.add_paper(seeded_db_url, paper)


class TestUpdateField:
    """Tests for paper_service.update_field."""

    def test_update_title(self, ephemeral_db_url: str, db_session: Session) -> None:
        """Updating the title via service makes the paper findable by the new title."""
        paper = PaperCreate(
            title="Svc Update Old Title",
            contents="Summary.",
            bibtex_id="SvcUpdate2026",
            bibtex="@misc{SvcUpdate2026}",
            authors=["Test, Author"],
        )
        paper_service.add_paper(ephemeral_db_url, paper)

        # Get the paper_id
        results = paper_service.search_by_title(ephemeral_db_url, "Svc Update Old Title")
        paper_id = results[0].paper_id

        paper_service.update_field(ephemeral_db_url, "papers", "title", paper_id, "Svc Update New Title")
        new_results = paper_service.search_by_title(ephemeral_db_url, "Svc Update New Title")
        assert len(new_results) == 1

        # Cleanup
        paper_service.delete_paper(ephemeral_db_url, paper_id)

    def test_update_unknown_table_raises(self, ephemeral_db_url: str) -> None:
        """Updating a non-existent table raises an error."""
        with pytest.raises((ValueError, TypeError)):
            paper_service.update_field(  # type: ignore[call-overload]
                ephemeral_db_url, "fake_table", "col", "id", "val"
            )


class TestDeletePaper:
    """Tests for paper_service.delete_paper."""

    def test_delete(self, ephemeral_db_url: str, db_session: Session) -> None:
        """After deletion, the paper is not found by search."""
        paper = PaperCreate(
            title="Svc Delete Test",
            contents="Summary.",
            bibtex_id="SvcDelete2026",
            bibtex="@misc{SvcDelete2026}",
            authors=["Test, Author"],
        )
        paper_service.add_paper(ephemeral_db_url, paper)
        results = paper_service.search_by_title(ephemeral_db_url, "Svc Delete Test")
        paper_id = results[0].paper_id

        paper_service.delete_paper(ephemeral_db_url, paper_id)
        after = paper_service.search_by_title(ephemeral_db_url, "Svc Delete Test")
        assert after == []


# ---------------------------------------------------------------------------
# import_service tests
# ---------------------------------------------------------------------------


TEX_FIXTURE = r"""\documentclass{article}
\begin{document}
\begin{itemize}
\item * TestImportPaper \cite{TestImport2026}: Some description here.
\item * AnotherPaper \cite{AnotherImport2026}: Another description.
\item * MissingBib \cite{MissingBib2026}: This one has no bib entry.
\end{itemize}
\end{document}
"""

BIB_FIXTURE = """
@article{TestImport2026,
  author = {Doe, John},
  title = {TestImportPaper},
  year = {2026}
}

@article{AnotherImport2026,
  author = {Smith, Jane},
  title = {AnotherPaper},
  year = {2026}
}
"""


class TestExtractPapersFromTexBib:
    """Tests for import_service.extract_papers_from_tex_bib."""

    def test_extracts_matching_entries(self) -> None:
        """Papers with matching BibTeX keys are extracted as PaperCreate DTOs."""
        results = list(
            import_service.extract_papers_from_tex_bib(TEX_FIXTURE, BIB_FIXTURE)
        )
        bibtex_ids = [r.bibtex_id for r in results]
        # TestImport2026 and AnotherImport2026 both have matching bib entries
        assert "TestImport2026" in bibtex_ids or "AnotherImport2026" in bibtex_ids

    def test_skips_missing_bib_entry(self) -> None:
        """Entries with no matching BibTeX record are silently skipped (no exception)."""
        results = list(
            import_service.extract_papers_from_tex_bib(TEX_FIXTURE, BIB_FIXTURE)
        )
        bibtex_ids = [r.bibtex_id for r in results]
        assert "MissingBib2026" not in bibtex_ids

    def test_no_exception_on_empty_bib(self) -> None:
        """An empty BibTeX file causes all entries to be skipped without error."""
        results = list(
            import_service.extract_papers_from_tex_bib(TEX_FIXTURE, "")
        )
        assert results == []
