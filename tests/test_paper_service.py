"""Service-layer tests for :mod:`paper_sorts.services.paper_service`.

Run against the real ephemeral database (the service opens its own sessions), so
no mocking of the session, repositories, or driver.
"""

from __future__ import annotations

import pytest
from sqlalchemy import Engine, select

from paper_sorts.db.models import Author
from paper_sorts.db.repositories import (
    DuplicateBibtexError,
    PaperCreate,
    PaperNotFoundError,
)
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service


def test_search_by_title_and_author(seeded_engine: Engine) -> None:
    """The service surfaces repository search results."""
    by_title = paper_service.search_by_title(
        seeded_engine, "Direct speech-to-speech translation with discrete units"
    )
    assert len(by_title) == 1
    by_author = paper_service.search_by_author(seeded_engine, "Pino, J.")
    assert len(by_author) == 2


def test_add_paper_then_retrievable(seeded_engine: Engine) -> None:
    """A paper added through the service is retrievable afterward."""
    paper_service.add_paper(
        seeded_engine,
        PaperCreate(
            title="Service-added paper",
            contents="Added via the service layer.",
            bibtex_id="Svc2026Add",
            bibtex="@article{Svc2026Add, title={Service-added paper}}",
            authors=["Svc, Author"],
        ),
    )
    assert paper_service.search_by_title(seeded_engine, "Service-added paper")
    assert paper_service.search_by_author(seeded_engine, "Svc, Author")


def test_add_duplicate_bibtex_raises(seeded_engine: Engine) -> None:
    """Adding a duplicate BibTeX key raises a typed domain error."""
    with pytest.raises(DuplicateBibtexError):
        paper_service.add_paper(
            seeded_engine,
            PaperCreate(
                title="Dup",
                contents="x",
                bibtex_id="Wang2021LargeScaleSA",
                bibtex="@article{Dup}",
                authors=["A, B"],
            ),
        )


def test_update_field_title(seeded_engine: Engine) -> None:
    """Updating papers.title through the service persists."""
    paper_id = paper_service.search_by_title(
        seeded_engine, "Direct speech-to-speech translation with discrete units"
    )[0].paper_id
    paper_service.update_field(seeded_engine, "papers", "title", "Service renamed", str(paper_id))
    assert paper_service.search_by_title(seeded_engine, "Service renamed")


def test_update_field_bibtex(seeded_engine: Engine) -> None:
    """Updating bib.bibtex through the service persists."""
    paper_service.update_field(
        seeded_engine,
        "bib",
        "bibtex",
        "@article{Wang2021LargeScaleSA, note={svc-updated}}",
        "Wang2021LargeScaleSA",
    )
    summary = paper_service.search_by_title(
        seeded_engine,
        "Large-scale Self- an Semi-Supervised learning for speech translation",
    )[0]
    assert "svc-updated" in summary.bibtex


def test_update_field_author(seeded_engine: Engine) -> None:
    """Updating authors_id.author through the service persists."""
    with with_session(seeded_engine) as session:
        author_id = session.execute(
            select(Author.id).where(Author.author == "Gu, Jiatao")
        ).scalar_one()
    paper_service.update_field(seeded_engine, "authors_id", "author", "Gu, J.", str(author_id))
    assert paper_service.search_by_author(seeded_engine, "Gu, J.")


def test_update_field_rejects_id_column(seeded_engine: Engine) -> None:
    """ID columns are never editable."""
    with pytest.raises(paper_service.UnknownColumnError):
        paper_service.update_field(seeded_engine, "papers", "id", "5", "1")
    with pytest.raises(paper_service.UnknownColumnError):
        paper_service.update_field(seeded_engine, "authors_papers", "author_id", "5", "1")


def test_update_field_rejects_authors_papers(seeded_engine: Engine) -> None:
    """The authors_papers link table has no editable column."""
    with pytest.raises(paper_service.UnknownColumnError):
        paper_service.update_field(seeded_engine, "authors_papers", "paper", "x", "1")


def test_update_field_rejects_unknown_column(seeded_engine: Engine) -> None:
    """A non-editable column on a valid table is rejected."""
    with pytest.raises(paper_service.UnknownColumnError):
        paper_service.update_field(seeded_engine, "papers", "summary", "x", "1")
    with pytest.raises(paper_service.UnknownColumnError):
        paper_service.update_field(seeded_engine, "bib", "bibtex_id", "x", "k")


def test_delete_paper(seeded_engine: Engine) -> None:
    """Deleting a paper through the service removes it."""
    target = next(
        p
        for p in paper_service.search_by_title(seeded_engine, "Attention is all you need")
        if p.bibtex_id == "Doe2020Attention"
    ).paper_id
    paper_service.delete_paper(seeded_engine, target)
    remaining = paper_service.search_by_title(seeded_engine, "Attention is all you need")
    assert {r.bibtex_id for r in remaining} == {"Vaswani2017Attention"}


def test_delete_missing_paper_raises(seeded_engine: Engine) -> None:
    """Deleting a non-existent paper raises."""
    with pytest.raises(PaperNotFoundError):
        paper_service.delete_paper(seeded_engine, 99999)
