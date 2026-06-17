"""Persistence-layer integration tests against a real PostgreSQL.

These exercise the repositories directly (no mocking of the session, repository,
or driver). Assertions reference :data:`tests.fixtures.seed_papers.SEED_PAPERS`.
"""

from __future__ import annotations

import pytest
from sqlalchemy import Engine, select

from paper_sorts.db.models import Author, Bib, Paper
from paper_sorts.db.repositories import (
    AuthorRepository,
    BibRepository,
    DuplicateBibtexError,
    PaperCreate,
    PaperNotFoundError,
    PaperRepository,
)
from paper_sorts.db.session import with_session


def test_search_by_title_single_match(seeded_engine: Engine) -> None:
    """A unique title returns exactly one summary with joined authors."""
    with with_session(seeded_engine) as session:
        results = PaperRepository(session).search_by_title(
            "Direct speech-to-speech translation with discrete units"
        )
    assert len(results) == 1
    summary = results[0]
    assert summary.bibtex_id == "Lee2022DirectS2ST"
    assert summary.authors.startswith("Lee, Ann and Chen, Peng-Jen")
    assert "Pino, J." in summary.authors


def test_search_by_title_multiple_matches(seeded_engine: Engine) -> None:
    """A shared title returns several summaries (disambiguation path)."""
    with with_session(seeded_engine) as session:
        results = PaperRepository(session).search_by_title("Attention is all you need")
    assert len(results) == 2
    assert {r.bibtex_id for r in results} == {"Vaswani2017Attention", "Doe2020Attention"}


def test_search_by_author(seeded_engine: Engine) -> None:
    """Searching by author returns that author's papers."""
    with with_session(seeded_engine) as session:
        results = PaperRepository(session).search_by_author("Pino, J.")
    titles = {r.title for r in results}
    assert "Large-scale Self- an Semi-Supervised learning for speech translation" in titles
    assert "Direct speech-to-speech translation with discrete units" in titles


def test_search_by_unknown_author_is_empty(seeded_engine: Engine) -> None:
    """An unknown author yields no results."""
    with with_session(seeded_engine) as session:
        assert PaperRepository(session).search_by_author("Nobody, X.") == []


def test_add_and_retrieve(seeded_engine: Engine) -> None:
    """An added paper is retrievable by both title and author."""
    new = PaperCreate(
        title="A brand new paper",
        contents="Fresh contents.",
        bibtex_id="New2026Paper",
        bibtex="@article{New2026Paper, title={A brand new paper}}",
        authors=["Fresh, Author"],
    )
    with with_session(seeded_engine) as session:
        PaperRepository(session).add(new)
    with with_session(seeded_engine) as session:
        repo = PaperRepository(session)
        assert repo.search_by_title("A brand new paper")[0].bibtex_id == "New2026Paper"
        assert repo.search_by_author("Fresh, Author")[0].title == "A brand new paper"


def test_add_duplicate_bibtex_key_raises(seeded_engine: Engine) -> None:
    """Adding a paper with an existing BibTeX key raises."""
    dup = PaperCreate(
        title="Dup",
        contents="x",
        bibtex_id="Wang2021LargeScaleSA",
        bibtex="@article{Other}",
        authors=["A, B"],
    )
    with pytest.raises(DuplicateBibtexError):  # noqa: PT012
        with with_session(seeded_engine) as session:
            PaperRepository(session).add(dup)


def test_update_title_and_contents(seeded_engine: Engine) -> None:
    """Title and contents updates persist."""
    with with_session(seeded_engine) as session:
        repo = PaperRepository(session)
        paper_id = repo.search_by_title("Direct speech-to-speech translation with discrete units")[
            0
        ].paper_id
        repo.update_title(paper_id, "Renamed title")
        repo.update_contents(paper_id, "Renamed contents")
    with with_session(seeded_engine) as session:
        summary = PaperRepository(session).search_by_title("Renamed title")[0]
    assert summary.contents == "Renamed contents"


def test_update_title_missing_raises(seeded_engine: Engine) -> None:
    """Updating a non-existent paper raises."""
    with pytest.raises(PaperNotFoundError):  # noqa: PT012
        with with_session(seeded_engine) as session:
            PaperRepository(session).update_title(99999, "x")


def test_update_bibtex(seeded_engine: Engine) -> None:
    """A BibTeX source update persists."""
    with with_session(seeded_engine) as session:
        BibRepository(session).update_bibtex(
            "Wang2021LargeScaleSA", "@article{Wang2021LargeScaleSA, note={updated}}"
        )
    with with_session(seeded_engine) as session:
        bib = session.get(Bib, "Wang2021LargeScaleSA")
        assert bib is not None and "updated" in (bib.bibtex or "")


def test_rename_author(seeded_engine: Engine) -> None:
    """Renaming an author moves their papers under the new name."""
    with with_session(seeded_engine) as session:
        author_id = session.execute(
            select(Author.id).where(Author.author == "Gu, Jiatao")
        ).scalar_one()
        AuthorRepository(session).rename(author_id, "Gu, J.")
    with with_session(seeded_engine) as session:
        assert PaperRepository(session).search_by_author("Gu, J.")


def test_delete_removes_paper_and_orphan_author(seeded_engine: Engine) -> None:
    """Deleting a paper removes it, its bib row, and any orphaned authors."""
    with with_session(seeded_engine) as session:
        repo = PaperRepository(session)
        paper_id = repo.search_by_title("Attention is all you need")  # two rows
        target = next(p for p in paper_id if p.bibtex_id == "Doe2020Attention").paper_id
        repo.delete(target)
    with with_session(seeded_engine) as session:
        repo = PaperRepository(session)
        remaining = repo.search_by_title("Attention is all you need")
        assert {r.bibtex_id for r in remaining} == {"Vaswani2017Attention"}
        # Jane Doe had only that paper -> orphan removed
        assert session.get(Bib, "Doe2020Attention") is None
        assert (
            session.execute(select(Author).where(Author.author == "Doe, Jane")).first()
            is None
        )
        # Shared author survives via the other paper
        assert session.execute(
            select(Paper).where(Paper.bibtex_id == "Vaswani2017Attention")
        ).first()
