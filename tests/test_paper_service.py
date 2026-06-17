"""Service-layer tests, including ``update_field`` dispatch and rejections."""

from __future__ import annotations

import pytest
from sqlalchemy import Engine

from paper_sorts.db.repositories import PaperCreate
from paper_sorts.services.paper_service import (
    PaperService,
    reject_authors_papers_update,
)


def _add(service: PaperService, key: str = "svc") -> int:
    """Add a baseline paper and return its id.

    :param service: the service under test.
    :param key: the BibTeX key to use.
    :returns: the new paper id.
    """
    return service.add_paper(
        PaperCreate(
            title="Svc Title",
            contents="svc contents",
            bibtex_id=key,
            bibtex=f"@misc{{{key}}}",
            authors=["Author, A"],
        )
    )


def test_update_field_papers_title(engine: Engine) -> None:
    """``update_field`` on papers/title persists the new title."""
    service = PaperService(engine)
    pid = _add(service)
    service.update_field("papers", "title", str(pid), "Renamed")
    assert service.search_by_title("Renamed")[0].contents == "svc contents"


def test_update_field_rejects_id_columns(engine: Engine) -> None:
    """Any ``*_id`` column is rejected before touching the database."""
    service = PaperService(engine)
    with pytest.raises(ValueError, match="IDs are unique"):
        service.update_field("papers", "paper_id", "1", "x")


def test_update_field_bib_wrong_column(engine: Engine) -> None:
    """A non-``bibtex`` column on the bib table is rejected."""
    service = PaperService(engine)
    _add(service, "bibk")
    with pytest.raises(ValueError, match="table bib"):
        service.update_field("bib", "title", "bibk", "x")


def test_update_field_authors_wrong_column(engine: Engine) -> None:
    """A non-``author`` column on authors_id is rejected."""
    service = PaperService(engine)
    _add(service, "authk")
    with pytest.raises(ValueError, match="table authors_id"):
        service.update_field("authors_id", "name", "Author, A", "x")


def test_update_field_author_rename(engine: Engine) -> None:
    """Renaming an author via the service updates search results."""
    service = PaperService(engine)
    _add(service, "renk")
    service.update_field("authors_id", "author", "Author, A", "Renamed, R")
    assert service.search_by_author("Author, A") == []
    assert len(service.search_by_author("Renamed, R")) == 1


def test_reject_authors_papers_update() -> None:
    """The link-table update helper always raises (legacy parity)."""
    with pytest.raises(ValueError, match="authors_papers"):
        reject_authors_papers_update()


def test_search_then_add_then_delete(engine: Engine) -> None:
    """End-to-end add/search/delete through the service layer."""
    service = PaperService(engine)
    _add(service, "flow")
    assert len(service.search_by_title("Svc Title")) == 1
    service.delete_paper("flow")
    assert service.search_by_title("Svc Title") == []
