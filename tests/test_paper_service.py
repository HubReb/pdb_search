"""Service-layer tests for update_field accept/reject paths and round-trips."""

from __future__ import annotations

from typing import cast

import pytest
from sqlalchemy import Engine

from paper_sorts.db.repositories import PaperCreate
from paper_sorts.services import paper_service
from paper_sorts.services.paper_service import UpdatableTable


def _add_one(engine: Engine) -> int:
    paper = PaperCreate(
        title="Original Title",
        summary="Original summary.",
        authors=["First, Auth"],
        bibtex_id="Svc2026",
        bibtex="@a{Svc2026}",
    )
    return paper_service.add_paper(engine, paper)


def test_update_title_persists(engine: Engine) -> None:
    paper_id = _add_one(engine)
    paper_service.update_field(engine, "papers", "title", str(paper_id), "New Title")
    results = paper_service.search_by_title(engine, "New Title")
    assert results and results[0].paper_id == paper_id


def test_update_contents_persists(engine: Engine) -> None:
    paper_id = _add_one(engine)
    paper_service.update_field(engine, "papers", "contents", str(paper_id), "Edited")
    summary = paper_service.search_by_title(engine, "Original Title")
    assert summary and summary[0].summary == "Edited"


def test_update_rejects_id_column(engine: Engine) -> None:
    with pytest.raises(ValueError, match="IDs are unique"):
        paper_service.update_field(engine, "papers", "paper_id", "1", "x")


def test_update_rejects_unknown_column(engine: Engine) -> None:
    paper_id = _add_one(engine)
    with pytest.raises(ValueError, match="not present in table papers"):
        paper_service.update_field(engine, "papers", "nonexistent", str(paper_id), "x")


def test_update_rejects_authors_papers_table(engine: Engine) -> None:
    # authors_papers is not in the UpdatableTable Literal; with a non-id column
    # it falls through to the assert_never branch, which raises at runtime.
    with pytest.raises((ValueError, AssertionError)):
        paper_service.update_field(
            engine, cast(UpdatableTable, "authors_papers"), "linkcol", "1", "x"
        )


def test_add_and_delete_round_trip(engine: Engine) -> None:
    paper_id = _add_one(engine)
    assert paper_service.search_by_author(engine, "First, Auth")[0].paper_id == paper_id
    paper_service.delete_paper(engine, paper_id)
    assert paper_service.search_by_author(engine, "First, Auth") == []


def test_update_bibtex_via_service(engine: Engine) -> None:
    _add_one(engine)
    paper_service.update_field(engine, "bib", "bibtex", "Svc2026", "@a{Svc2026, note={new}}")
    results = paper_service.search_by_title(engine, "Original Title")
    assert results[0].bibtex == "@a{Svc2026, note={new}}"


def test_update_bib_rejects_wrong_column(engine: Engine) -> None:
    _add_one(engine)
    with pytest.raises(ValueError, match="not present in table bibtex"):
        paper_service.update_field(engine, "bib", "title", "Svc2026", "x")


def test_update_author_via_service(engine: Engine) -> None:
    paper_id = _add_one(engine)
    paper_service.update_field(engine, "authors_id", "author", "First, Auth", "Renamed, Auth")
    assert paper_service.search_by_author(engine, "Renamed, Auth")[0].paper_id == paper_id
    assert paper_service.search_by_author(engine, "First, Auth") == []


def test_update_author_rejects_wrong_column(engine: Engine) -> None:
    _add_one(engine)
    with pytest.raises(ValueError, match="not present in table authors_id"):
        paper_service.update_field(engine, "authors_id", "name", "First, Auth", "x")


def test_update_paper_malformed_id(engine: Engine) -> None:
    with pytest.raises(ValueError, match="not a numeric id"):
        paper_service.update_field(engine, "papers", "title", "not-a-number", "x")
