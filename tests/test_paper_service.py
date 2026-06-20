"""Integration tests for paper_service.

These tests run against the real ephemeral PostgreSQL database.
No mocking of SQLAlchemy session or repositories (Principle II).
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

_LARGE_SCALE_TITLE = (
    "Large-scale Self- an Semi-Supervised learning for speech translation"
)


class TestSearchByTitle:
    def test_found(self, db_engine: object) -> None:
        results = search_by_title(db_engine, _LARGE_SCALE_TITLE)  # type: ignore[arg-type]
        assert isinstance(results, list)

    def test_not_found_returns_empty(self, db_engine: object) -> None:
        results = search_by_title(db_engine, "nonexistent title xyz")  # type: ignore[arg-type]
        assert results == []

    def test_known_title(self, db_engine: object) -> None:
        results = search_by_title(db_engine, _LARGE_SCALE_TITLE)  # type: ignore[arg-type]
        assert len(results) >= 1
        assert results[0].bibtex_id == "Wang2021LargeScaleSA"


class TestSearchByAuthor:
    def test_found(self, db_engine: object) -> None:
        results = search_by_author(db_engine, "Pino, J.")  # type: ignore[arg-type]
        assert len(results) >= 1

    def test_not_found_returns_empty(self, db_engine: object) -> None:
        results = search_by_author(db_engine, "Nobody, N.")  # type: ignore[arg-type]
        assert results == []


class TestAddPaper:
    def test_add_and_retrieve(self, db_engine: object) -> None:
        paper = PaperCreate(
            title="Service Add Test",
            contents="summary",
            bibtex_id="SvcAdd2026",
            bibtex="@article{SvcAdd2026, author={T.}, title={Service Add Test}}",
            authors=["T., Author"],
        )
        add_paper(db_engine, paper)  # type: ignore[arg-type]
        results = search_by_title(db_engine, "Service Add Test")  # type: ignore[arg-type]
        assert len(results) == 1

    def test_duplicate_raises(self, db_engine: object) -> None:
        paper = PaperCreate(
            title="Dup Service",
            contents="x",
            bibtex_id="Wang2021LargeScaleSA",
            bibtex="@article{Wang2021LargeScaleSA, author={X}}",
            authors=["X"],
        )
        with pytest.raises(ValueError, match="already exists"):
            add_paper(db_engine, paper)  # type: ignore[arg-type]


class TestUpdateField:
    def test_update_title(self, db_engine: object) -> None:
        paper = PaperCreate(
            title="Title Before Update",
            contents="summary",
            bibtex_id="UpdTitle2026",
            bibtex="@article{UpdTitle2026, author={U.}, title={Before}}",
            authors=["U., Author"],
        )
        add_paper(db_engine, paper)  # type: ignore[arg-type]
        from paper_sorts.db.repositories import PaperRepository
        from paper_sorts.db.session import with_session

        with with_session(db_engine) as session:  # type: ignore[arg-type]
            results = PaperRepository.search_by_title(session, "Title Before Update")
        paper_id = str(results[0].paper_id)

        update_field(db_engine, "papers", paper_id, "title", "Title After Update")  # type: ignore[arg-type]
        results2 = search_by_title(db_engine, "Title After Update")  # type: ignore[arg-type]
        assert len(results2) == 1

    def test_update_invalid_field_raises(self, db_engine: object) -> None:
        with pytest.raises(ValueError, match="Cannot update field"):
            update_field(db_engine, "papers", "1", "nonexistent_field", "value")  # type: ignore[arg-type]

    def test_update_bib(self, db_engine: object) -> None:
        paper = PaperCreate(
            title="Bib Update Test",
            contents="s",
            bibtex_id="BibUpd2026",
            bibtex="@article{BibUpd2026, author={B.}, title={Bib}}",
            authors=["B., Author"],
        )
        add_paper(db_engine, paper)  # type: ignore[arg-type]
        new_bibtex = "@article{BibUpd2026, author={C.}, title={Updated Bib}}"
        update_field(db_engine, "bib", "BibUpd2026", "bibtex", new_bibtex)  # type: ignore[arg-type]
        results = search_by_title(db_engine, "Bib Update Test")  # type: ignore[arg-type]
        assert results[0].bibtex == new_bibtex

    def test_update_author(self, db_engine: object) -> None:
        paper = PaperCreate(
            title="Author Update Test",
            contents="s",
            bibtex_id="AuthUpd2026",
            bibtex="@article{AuthUpd2026, author={Old.}}",
            authors=["Old, AuthorSvc"],
        )
        add_paper(db_engine, paper)  # type: ignore[arg-type]
        update_field(db_engine, "authors_id", "Old, AuthorSvc", "author", "New, AuthorSvc")  # type: ignore[arg-type]
        results = search_by_author(db_engine, "New, AuthorSvc")  # type: ignore[arg-type]
        assert any(r.bibtex_id == "AuthUpd2026" for r in results)


class TestDeletePaper:
    def test_delete(self, db_engine: object) -> None:
        paper = PaperCreate(
            title="Delete Service Test",
            contents="s",
            bibtex_id="DelSvc2026",
            bibtex="@article{DelSvc2026, author={D.}}",
            authors=["D., Author"],
        )
        add_paper(db_engine, paper)  # type: ignore[arg-type]
        delete_paper(db_engine, "DelSvc2026")  # type: ignore[arg-type]
        results = search_by_title(db_engine, "Delete Service Test")  # type: ignore[arg-type]
        assert results == []

    def test_delete_not_found_raises(self, db_engine: object) -> None:
        with pytest.raises(ValueError, match="not found"):
            delete_paper(db_engine, "DoesNotExist9999")  # type: ignore[arg-type]
