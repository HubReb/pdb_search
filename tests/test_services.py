"""Real-DB tests for the service layer."""

from __future__ import annotations

import pytest
from sqlalchemy import Engine

from paper_sorts.db.repositories import PaperCreate
from paper_sorts.services.paper_service import PaperService


def test_search_by_title_single(seeded_engine: Engine) -> None:
    service = PaperService(seeded_engine)
    rows = service.search_by_title("Direct speech-to-speech translation with discrete units")
    assert len(rows) == 1
    assert rows[0].authors == ["Lee, Ann", "Chen, Peng-Jen", "Pino, J."]


def test_search_by_title_multi(seeded_engine: Engine) -> None:
    service = PaperService(seeded_engine)
    rows = service.search_by_title("Shared Title")
    assert len(rows) == 2


def test_search_by_author(seeded_engine: Engine) -> None:
    service = PaperService(seeded_engine)
    rows = service.search_by_author("Pino, J.")
    assert len(rows) >= 2


def test_add_paper(migrated_engine: Engine) -> None:
    service = PaperService(migrated_engine)
    service.add_paper(
        PaperCreate(
            title="Added",
            summary="x",
            bibtex_id="Add2026",
            bibtex="@misc{Add2026}",
            authors=["Lone, Wolf"],
        )
    )
    assert service.search_by_title("Added")[0].bibtex_id == "Add2026"


def test_add_duplicate_rejected(seeded_engine: Engine) -> None:
    service = PaperService(seeded_engine)
    with pytest.raises(ValueError):
        service.add_paper(
            PaperCreate(
                title="dup",
                summary="x",
                bibtex_id="Wang2021LargeScaleSA",
                bibtex="@misc{dup}",
                authors=["A, B"],
            )
        )


def test_update_title(seeded_engine: Engine) -> None:
    service = PaperService(seeded_engine)
    pid = service.search_by_title("Shared Title")[0].paper_id
    service.update_field("papers", "title", str(pid), "Renamed")
    assert service.search_by_title("Renamed")


def test_update_contents(seeded_engine: Engine) -> None:
    service = PaperService(seeded_engine)
    pid = service.search_by_title("Shared Title")[0].paper_id
    service.update_field("papers", "contents", str(pid), "new summary")
    rows = service.search_by_title("Shared Title")
    assert any(r.summary == "new summary" for r in rows)


def test_update_author(seeded_engine: Engine) -> None:
    from sqlalchemy import select

    from paper_sorts.db.models import Author
    from paper_sorts.db.session import with_session

    service = PaperService(seeded_engine)
    with with_session(seeded_engine) as s:
        author_id = s.scalar(select(Author.id).where(Author.author == "Alpha, Anne"))
    assert author_id is not None
    service.update_field("authors_id", "author", str(author_id), "Alpha, Annette")
    assert service.search_by_author("Alpha, Annette")


def test_update_rejects_id_column(seeded_engine: Engine) -> None:
    service = PaperService(seeded_engine)
    with pytest.raises(ValueError):
        service.update_field("papers", "paper_id", "1", "x")


def test_update_rejects_unknown_column(seeded_engine: Engine) -> None:
    service = PaperService(seeded_engine)
    with pytest.raises(ValueError):
        service.update_field("papers", "nonsense", "1", "x")


def test_delete_paper(seeded_engine: Engine) -> None:
    service = PaperService(seeded_engine)
    pid = service.search_by_title("Direct speech-to-speech translation with discrete units")[
        0
    ].paper_id
    service.delete_paper(pid)
    assert service.search_by_title("Direct speech-to-speech translation with discrete units") == []


def test_delete_missing_raises(seeded_engine: Engine) -> None:
    service = PaperService(seeded_engine)
    with pytest.raises(ValueError):
        service.delete_paper(999999)
