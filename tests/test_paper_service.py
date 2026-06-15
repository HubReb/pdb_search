"""Real-database tests for the service layer.

Exercises ``PaperService`` orchestration against the ephemeral PostgreSQL, asserting on rows
derived from ``SEED_PAPERS``. No mocking of the session or repositories (Principle II).
"""

from __future__ import annotations

import pytest
from sqlalchemy import Engine

from paper_sorts.db.repositories import PaperCreate
from paper_sorts.services.paper_service import PaperService


def test_search_by_title_and_author(seeded_engine: Engine) -> None:
    """Service search mirrors the repository search results."""
    service = PaperService(seeded_engine)
    by_title = service.search_by_title("Direct speech-to-speech translation with discrete units")
    assert len(by_title) == 1
    by_author = service.search_by_author("Pino, J.")
    assert len(by_author) >= 2


def test_add_paper_then_find(engine: Engine) -> None:
    """An added paper is retrievable by both author and title."""
    service = PaperService(engine)
    service.add_paper(
        PaperCreate(
            title="Service-added paper",
            summary="added via service",
            bibtex_id="Svc2024",
            bibtex="@article{Svc2024}",
            authors=["Author, Alpha", "Author, Beta"],
        )
    )
    assert len(service.search_by_title("Service-added paper")) == 1
    assert len(service.search_by_author("Author, Alpha")) == 1


def test_add_duplicate_key_raises(seeded_engine: Engine) -> None:
    """Adding a paper with an existing BibTeX key raises."""
    service = PaperService(seeded_engine)
    with pytest.raises(ValueError):
        service.add_paper(
            PaperCreate(
                title="dup",
                summary="dup",
                bibtex_id="Lee2022DirectS2ST",
                bibtex="@article{dup}",
                authors=["X, Y"],
            )
        )


def test_update_title_and_contents(seeded_engine: Engine) -> None:
    """Updating papers.title and papers.contents persists the new values."""
    service = PaperService(seeded_engine)
    paper = service.search_by_title("Direct speech-to-speech translation with discrete units")[0]
    service.update_field("papers", "title", "Renamed paper", str(paper.paper_id))
    service.update_field("papers", "contents", "new summary", str(paper.paper_id))
    renamed = service.search_by_title("Renamed paper")
    assert len(renamed) == 1
    assert renamed[0].summary == "new summary"


def test_update_bib(seeded_engine: Engine) -> None:
    """Updating the bibtex source persists for the key."""
    service = PaperService(seeded_engine)
    service.update_field("bib", "bibtex", "@misc{changed}", "Lee2022DirectS2ST")
    updated = service.search_by_title("Direct speech-to-speech translation with discrete units")
    assert updated[0].bibtex == "@misc{changed}"


def test_update_author_name(seeded_engine: Engine) -> None:
    """Renaming an author moves their papers to the new name."""
    service = PaperService(seeded_engine)
    service.update_field("authors_id", "author", "Smith, Johnny", "Smith, John")
    assert len(service.search_by_author("Smith, Johnny")) == 1
    assert service.search_by_author("Smith, John") == []


def test_update_bib_wrong_column_raises(seeded_engine: Engine) -> None:
    """Only the bibtex column is editable in the bib table."""
    service = PaperService(seeded_engine)
    with pytest.raises(ValueError):
        service.update_field("bib", "bibtex_id", "x", "Lee2022DirectS2ST")


def test_delete_paper(seeded_engine: Engine) -> None:
    """Deleting a paper removes it and reports success; missing id reports False."""
    service = PaperService(seeded_engine)
    paper = service.search_by_title(
        "Large-scale Self- and Semi-Supervised learning for speech translation"
    )[0]
    assert service.delete_paper(paper.paper_id) is True
    assert (
        service.search_by_title(
            "Large-scale Self- and Semi-Supervised learning for speech translation"
        )
        == []
    )
    assert service.delete_paper(999999) is False
