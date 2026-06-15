"""Real-database tests for the persistence layer.

These run against the ephemeral PostgreSQL provisioned in ``conftest.py`` and assert on rows
produced by ``tests/fixtures/seed_papers.SEED_PAPERS``. The SQLAlchemy session and repositories
are exercised directly — never mocked (constitution Principle II).
"""

from __future__ import annotations

import pytest
from sqlalchemy import Engine

from paper_sorts.db.repositories import (
    AuthorRepository,
    BibRepository,
    PaperRepository,
)
from paper_sorts.db.session import with_session
from tests.fixtures.seed_papers import SEED_PAPERS


def test_search_by_title_single_match(seeded_engine: Engine) -> None:
    """A unique title returns exactly one fully populated summary."""
    with with_session(seeded_engine) as session:
        results = PaperRepository(session).search_by_title(
            "Direct speech-to-speech translation with discrete units"
        )
    assert len(results) == 1
    summary = results[0]
    assert summary.bibtex_id == "Lee2022DirectS2ST"
    assert "Lee, Ann" in summary.authors
    assert "Pino, J." in summary.authors
    assert summary.bibtex.startswith("@article{Lee2022DirectS2ST")


def test_search_by_title_multiple_matches(seeded_engine: Engine) -> None:
    """A duplicated title returns one summary per matching paper."""
    with with_session(seeded_engine) as session:
        results = PaperRepository(session).search_by_title("A shared title")
    assert len(results) == 2
    assert {r.bibtex_id for r in results} == {"Smith2019Shared", "Doe2018Shared"}


def test_search_by_title_no_match(seeded_engine: Engine) -> None:
    """An unknown title returns an empty list."""
    with with_session(seeded_engine) as session:
        assert PaperRepository(session).search_by_title("no such title") == []


def test_search_by_author(seeded_engine: Engine) -> None:
    """An author on multiple papers returns all of them."""
    with with_session(seeded_engine) as session:
        results = PaperRepository(session).search_by_author("Pino, J.")
    titles = {r.title for r in results}
    assert "Direct speech-to-speech translation with discrete units" in titles
    assert "Large-scale Self- and Semi-Supervised learning for speech translation" in titles


def test_bibtex_accents_round_trip(seeded_engine: Engine) -> None:
    """LaTeX accents/escapes survive storage and retrieval unchanged."""
    with with_session(seeded_engine) as session:
        results = PaperRepository(session).search_by_title(
            "Schöne Grüße: accents \\& escapes in BibTeX"
        )
    assert len(results) == 1
    assert '{\\"o}' in results[0].bibtex
    assert "\\&" in results[0].bibtex
    assert "{Pino}" in results[0].bibtex


def test_add_and_retrieve(engine: Engine) -> None:
    """A freshly added paper is retrievable by title and author."""
    with with_session(engine) as session:
        bibs = BibRepository(session)
        papers = PaperRepository(session)
        authors = AuthorRepository(session)
        bibs.add("New2024", "@article{New2024, title={New}, author={Ng, Nina}}")
        paper_id = papers.add_paper_row("A new paper", "summary", "New2024")
        authors.link(authors.get_or_create_author_id("Ng, Nina"), paper_id)
    with with_session(engine) as session:
        by_title = PaperRepository(session).search_by_title("A new paper")
        by_author = PaperRepository(session).search_by_author("Ng, Nina")
    assert len(by_title) == 1
    assert len(by_author) == 1
    assert by_title[0].bibtex_id == "New2024"


def test_update_bibtex_unique_violation(seeded_engine: Engine) -> None:
    """Updating a bibtex to an existing value raises (UNIQUE constraint)."""
    existing = SEED_PAPERS[1].bibtex
    with with_session(seeded_engine) as session:
        repo = BibRepository(session)
        with pytest.raises(ValueError):
            repo.update_bibtex(SEED_PAPERS[0].bibtex_id, existing)


def test_delete_paper_orphans_author(engine: Engine) -> None:
    """Deleting the only paper of an author removes the author row too."""
    with with_session(engine) as session:
        bibs = BibRepository(session)
        papers = PaperRepository(session)
        authors = AuthorRepository(session)
        bibs.add("Solo2024", "@article{Solo2024}")
        paper_id = papers.add_paper_row("Solo paper", "s", "Solo2024")
        authors.link(authors.get_or_create_author_id("Lone, Larry"), paper_id)
    with with_session(engine) as session:
        authors = AuthorRepository(session)
        authors.unlink_for_paper("Lone, Larry", paper_id)
        PaperRepository(session).delete_paper(paper_id)
        BibRepository(session).delete("Solo2024")
    with with_session(engine) as session:
        assert PaperRepository(session).search_by_author("Lone, Larry") == []
