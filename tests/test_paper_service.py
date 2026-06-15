"""Integration tests for :class:`PaperService` over a real database.

Each operation runs in its own committed transaction, so these tests assert on
state persisted across separate sessions. Seed rows trace to
``tests/fixtures/seed_papers.SEED_PAPERS``.
"""

from __future__ import annotations

import pytest
from sqlalchemy import Engine

from paper_sorts.db.repositories import (
    DuplicateBibtexKeyError,
    PaperCreate,
)
from paper_sorts.db.session import with_session
from paper_sorts.services.paper_service import PaperService
from tests.conftest import _seed


@pytest.fixture
def service(engine: Engine) -> PaperService:
    """A PaperService over a freshly seeded ephemeral database."""
    with with_session(engine) as session:
        _seed(session)
    return PaperService(engine)


def test_add_then_search_round_trip(service: PaperService) -> None:
    """A paper added through the service is found by title and author."""
    service.add_paper(
        PaperCreate(
            title="Service Round Trip",
            authors=["Author, A"],
            summary="round trip",
            bibtex_id="RT1",
            bibtex="@misc{RT1}",
        )
    )
    assert service.search_by_title("Service Round Trip")[0].authors == ["Author, A"]
    assert service.search_by_author("Author, A")[0].title == "Service Round Trip"


def test_add_duplicate_key_raises(service: PaperService) -> None:
    """Adding a duplicate BibTeX key raises and writes nothing."""
    with pytest.raises(DuplicateBibtexKeyError):
        service.add_paper(
            PaperCreate(
                title="Dup",
                authors=["X, Y"],
                summary="dup",
                bibtex_id="Lee2022Direct",
                bibtex="@misc{dup}",
            )
        )


def test_update_title_field(service: PaperService) -> None:
    """Updating the title via the service persists across sessions."""
    new_id = _add_editable(service)
    service.update_field("papers", "title", str(new_id), "Renamed")
    assert service.search_by_title("Renamed")[0].bibtex_id == "EDIT"


def test_update_contents_field(service: PaperService) -> None:
    """Updating contents via the service persists."""
    new_id = _add_editable(service)
    service.update_field("papers", "contents", str(new_id), "new summary")
    assert service.search_by_title("Editable")[0].summary == "new summary"


def test_update_rejects_noneditable_column(service: PaperService) -> None:
    """A non-editable column raises ValueError."""
    new_id = _add_editable(service)
    with pytest.raises(ValueError):
        service.update_field("papers", "id", str(new_id), "nope")


def test_update_bibtex(service: PaperService) -> None:
    """The bibtex field is editable via the bib table."""
    service.update_field("bib", "bibtex", "Lee2022Direct", "@misc{Lee2022Direct, new=true}")
    assert (
        "new=true"
        in service.search_by_title("Direct speech-to-speech translation with discrete units")[
            0
        ].bibtex
    )


def test_author_rename_merges(service: PaperService) -> None:
    """Renaming an author onto an existing name merges their papers."""
    service.update_field("authors_id", "author", "Wang, Changhan", "Pino, J.")
    titles = {p.title for p in service.search_by_author("Pino, J.")}
    assert "Large-scale Self- and Semi-Supervised learning for speech translation" in titles
    assert service.search_by_author("Wang, Changhan") == []


def test_delete_paper(service: PaperService) -> None:
    """Deleting a paper removes it from search results."""
    summary = service.search_by_title("Direct speech-to-speech translation with discrete units")[0]
    service.delete_paper(summary)
    assert service.search_by_title(summary.title) == []


def _add_editable(service: PaperService) -> int:
    """Add a throwaway paper and return its id (via a fresh search)."""
    service.add_paper(
        PaperCreate(
            title="Editable",
            authors=["Ed, Itor"],
            summary="before",
            bibtex_id="EDIT",
            bibtex="@misc{EDIT}",
        )
    )
    # Recover the id through the ORM by re-querying.
    from sqlalchemy import select

    from paper_sorts.db.models import Paper

    with with_session(service._engine) as session:
        return session.scalar(select(Paper.id).where(Paper.bibtex_id == "EDIT"))  # type: ignore[return-value]
