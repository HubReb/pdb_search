"""Integration tests for the persistence layer against a real PostgreSQL.

Per constitution Principle II, these run against the ephemeral database (no
mocking of the session, repositories, or driver) and assert against the
co-located ``SEED_PAPERS`` fixture.
"""

from __future__ import annotations

import pytest
from sqlalchemy import Engine

from paper_sorts.db.repositories import (
    AuthorRepository,
    BibRepository,
    PaperCreate,
    PaperRepository,
)
from paper_sorts.db.session import with_session


def test_search_by_author_single_match(seeded_engine: Engine) -> None:
    with with_session(seeded_engine) as session:
        results = PaperRepository(session).search_by_author("Wang, Changhan")
    assert len(results) == 1
    assert results[0].bibtex_id == "Wang2021LargeScaleSA"
    assert "Wang, Changhan" in results[0].authors


def test_search_by_author_joins_all_authors(seeded_engine: Engine) -> None:
    with with_session(seeded_engine) as session:
        results = PaperRepository(session).search_by_title(
            "Direct speech-to-speech translation with discrete units"
        )
    assert results[0].authors == "Lee, Ann and Chen, Peng-Jen and Pino, J."


def test_search_by_author_not_found_returns_empty(seeded_engine: Engine) -> None:
    with with_session(seeded_engine) as session:
        assert PaperRepository(session).search_by_author("Nobody, X.") == []


def test_search_by_title_duplicate_titles(seeded_engine: Engine) -> None:
    with with_session(seeded_engine) as session:
        results = PaperRepository(session).search_by_title("A Survey")
    assert len(results) == 2
    assert {r.bibtex_id for r in results} == {"Mueller2020Survey", "Smith2022Survey"}


def test_add_then_retrieve_by_title_and_author(engine: Engine) -> None:
    paper = PaperCreate(
        title="A New Paper",
        summary="Fresh summary.",
        authors=["Doe, Jane", "Roe, Rick"],
        bibtex_id="Doe2026New",
        bibtex="@article{Doe2026New, title={A New Paper}}",
    )
    with with_session(engine) as session:
        PaperRepository(session).add(paper)
    with with_session(engine) as session:
        repo = PaperRepository(session)
        assert repo.search_by_title("A New Paper")[0].bibtex_id == "Doe2026New"
        assert repo.search_by_author("Doe, Jane")[0].bibtex_id == "Doe2026New"


def test_add_duplicate_bibtex_key_raises(seeded_engine: Engine) -> None:
    dup = PaperCreate(
        title="dup",
        summary="s",
        authors=["X, Y"],
        bibtex_id="Wang2021LargeScaleSA",
        bibtex="something",
    )
    with pytest.raises(ValueError):  # noqa: PT011
        with with_session(seeded_engine) as session:
            PaperRepository(session).add(dup)


def test_delete_removes_paper_bib_and_orphan_authors(engine: Engine) -> None:
    paper = PaperCreate(
        title="Solo",
        summary="s",
        authors=["Only, Author"],
        bibtex_id="Solo2026",
        bibtex="@a{Solo2026}",
    )
    with with_session(engine) as session:
        paper_id = PaperRepository(session).add(paper)
    with with_session(engine) as session:
        PaperRepository(session).delete(paper_id)
    with with_session(engine) as session:
        repo = PaperRepository(session)
        assert repo.search_by_title("Solo") == []
        assert repo.search_by_author("Only, Author") == []


def test_get_by_id(seeded_engine: Engine) -> None:
    with with_session(seeded_engine) as session:
        repo = PaperRepository(session)
        first = repo.search_by_title(
            "Large-scale Self- and Semi-Supervised learning for speech translation"
        )[0]
        fetched = repo.get_by_id(first.paper_id)
    assert fetched is not None
    assert fetched.bibtex_id == "Wang2021LargeScaleSA"


def test_update_bibtex_unique_guard(seeded_engine: Engine) -> None:
    with with_session(seeded_engine) as session:
        BibRepository(session).update_bibtex("Lee2021Direct", "brand new source")
    with with_session(seeded_engine) as session:
        # Re-using an existing source string violates UNIQUE.
        with pytest.raises(ValueError):  # noqa: PT011
            BibRepository(session).update_bibtex("Lee2021Direct", "brand new source")


def test_author_rename_repoints_links(engine: Engine) -> None:
    paper = PaperCreate(
        title="Renamed",
        summary="s",
        authors=["Old, Name"],
        bibtex_id="Ren2026",
        bibtex="@a{Ren2026}",
    )
    with with_session(engine) as session:
        PaperRepository(session).add(paper)
    with with_session(engine) as session:
        AuthorRepository(session).rename("Old, Name", "New, Name")
    with with_session(engine) as session:
        repo = PaperRepository(session)
        assert repo.search_by_author("New, Name")[0].bibtex_id == "Ren2026"
        assert repo.search_by_author("Old, Name") == []
