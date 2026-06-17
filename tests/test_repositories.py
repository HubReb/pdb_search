"""Persistence-layer integration tests against a real ephemeral PostgreSQL.

Assertions reference rows from ``tests/fixtures/seed_papers.SEED_PAPERS``
(constitution Principle II). No mocking of the session, repositories, or driver.
"""

from __future__ import annotations

import pytest
from sqlalchemy import Engine

from paper_sorts.db.repositories import (
    DuplicateError,
    NotFoundError,
    PaperCreate,
    PaperRepository,
)
from paper_sorts.db.session import with_session


def test_search_by_author_returns_seeded_paper(seeded_engine: Engine) -> None:
    """Searching by ``Pino, J.`` returns the seeded speech-translation papers."""
    with with_session(seeded_engine) as session:
        results = PaperRepository(session).search_by_author("Pino, J.")
    titles = {r.title for r in results}
    assert "Large-scale Self- an Semi-Supervised learning for speech translation" in titles
    keys = {r.bibtex_id for r in results}
    assert "Wang2021LargeScaleSA" in keys


def test_search_by_author_missing_returns_empty(seeded_engine: Engine) -> None:
    """Searching an unknown author yields no results."""
    with with_session(seeded_engine) as session:
        assert PaperRepository(session).search_by_author("no author") == []


def test_search_by_title_joins_all_authors(seeded_engine: Engine) -> None:
    """A title search returns every author joined with `` and``."""
    with with_session(seeded_engine) as session:
        results = PaperRepository(session).search_by_title(
            "Direct speech-to-speech translation with discrete units"
        )
    assert len(results) == 1
    authors = results[0].authors
    assert " and " in authors
    for name in ("Lee, Ann", "Chen, Peng-Jen", "Pino, J.", "Hsu, Wei-Ning"):
        assert name in authors


def test_search_by_title_missing_returns_empty(seeded_engine: Engine) -> None:
    """A title search for a missing paper yields no results."""
    with with_session(seeded_engine) as session:
        assert PaperRepository(session).search_by_title("no title") == []


def test_add_and_delete_roundtrip(engine: Engine) -> None:
    """A paper can be added and then deleted, with author orphans removed."""
    create = PaperCreate(
        title="Add Test",
        contents="c",
        bibtex_id="addkey",
        bibtex="@misc{addkey}",
        authors=["Solo, H."],
    )
    with with_session(engine) as session:
        PaperRepository(session).add_paper(create)
    with with_session(engine) as session:
        assert len(PaperRepository(session).search_by_title("Add Test")) == 1
    with with_session(engine) as session:
        PaperRepository(session).delete_paper("addkey")
    with with_session(engine) as session:
        repo = PaperRepository(session)
        assert repo.search_by_title("Add Test") == []
        assert repo.search_by_author("Solo, H.") == []


def test_add_duplicate_key_raises(engine: Engine) -> None:
    """Re-adding an existing BibTeX key raises ``DuplicateError``."""
    create = PaperCreate(
        title="Dup", contents="c", bibtex_id="dk", bibtex="@misc{dk}", authors=["A, B"]
    )
    with with_session(engine) as session:
        PaperRepository(session).add_paper(create)
    with pytest.raises(DuplicateError):
        with with_session(engine) as session:
            PaperRepository(session).add_paper(create)


def test_delete_missing_raises(engine: Engine) -> None:
    """Deleting a non-existent paper raises ``NotFoundError``."""
    with pytest.raises(NotFoundError):
        with with_session(engine) as session:
            PaperRepository(session).delete_paper("ghost")


def test_update_title_and_contents(engine: Engine) -> None:
    """Paper title and contents update by id."""
    create = PaperCreate(
        title="Old", contents="old", bibtex_id="uk", bibtex="@misc{uk}", authors=["A, B"]
    )
    with with_session(engine) as session:
        pid = PaperRepository(session).add_paper(create)
    with with_session(engine) as session:
        PaperRepository(session).update_paper_field(pid, "title", "New")
        PaperRepository(session).update_paper_field(pid, "contents", "new")
    with with_session(engine) as session:
        results = PaperRepository(session).search_by_title("New")
    assert results[0].contents == "new"


def test_update_paper_unknown_column_raises(engine: Engine) -> None:
    """An unknown paper column raises ``ValueError``."""
    create = PaperCreate(
        title="X", contents="c", bibtex_id="xk", bibtex="@misc{xk}", authors=["A, B"]
    )
    with with_session(engine) as session:
        pid = PaperRepository(session).add_paper(create)
    with pytest.raises(ValueError):
        with with_session(engine) as session:
            PaperRepository(session).update_paper_field(pid, "nope", "v")


def test_update_bibtex_uniqueness(engine: Engine) -> None:
    """Updating a bib entry to a duplicate string raises ``DuplicateError``."""
    with with_session(engine) as session:
        repo = PaperRepository(session)
        repo.add_paper(
            PaperCreate(title="P1", contents="c", bibtex_id="b1", bibtex="one", authors=["A, B"])
        )
        repo.add_paper(
            PaperCreate(title="P2", contents="c", bibtex_id="b2", bibtex="two", authors=["C, D"])
        )
    with with_session(engine) as session:
        PaperRepository(session).bib.update_bibtex("b1", "updated-one")
    with pytest.raises(DuplicateError):
        with with_session(engine) as session:
            PaperRepository(session).bib.update_bibtex("b2", "updated-one")


def test_rename_author_merges_onto_existing(engine: Engine) -> None:
    """Renaming an author onto an existing name merges their papers."""
    with with_session(engine) as session:
        repo = PaperRepository(session)
        repo.add_paper(
            PaperCreate(title="P1", contents="c", bibtex_id="b1", bibtex="one", authors=["Old, O"])
        )
        repo.add_paper(
            PaperCreate(title="P2", contents="c", bibtex_id="b2", bibtex="two", authors=["New, N"])
        )
    with with_session(engine) as session:
        PaperRepository(session).authors.rename_author("Old, O", "New, N")
    with with_session(engine) as session:
        repo = PaperRepository(session)
        assert repo.search_by_author("Old, O") == []
        assert len(repo.search_by_author("New, N")) == 2
