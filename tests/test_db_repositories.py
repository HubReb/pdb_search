"""Real-DB tests for the repository layer.

Assertions reference rows in ``tests/fixtures/seed_papers.SEED_PAPERS``.
"""

from __future__ import annotations

from sqlalchemy import Engine

from paper_sorts.db.repositories import (
    AuthorRepository,
    BibRepository,
    PaperCreate,
    PaperRepository,
)
from paper_sorts.db.session import with_session


def test_get_by_title_single(seeded_engine: Engine) -> None:
    repo = PaperRepository()
    with with_session(seeded_engine) as s:
        rows = repo.get_by_title(
            s, "Large-scale Self- an Semi-Supervised learning for speech translation"
        )
    assert len(rows) == 1
    assert rows[0].bibtex_id == "Wang2021LargeScaleSA"
    assert rows[0].authors == ["Wang, Changhan", "Pino, J."]


def test_get_by_title_shared(seeded_engine: Engine) -> None:
    repo = PaperRepository()
    with with_session(seeded_engine) as s:
        rows = repo.get_by_title(s, "Shared Title")
    assert {r.bibtex_id for r in rows} == {"Shared2020A", "Shared2021B"}


def test_get_by_title_missing(seeded_engine: Engine) -> None:
    repo = PaperRepository()
    with with_session(seeded_engine) as s:
        assert repo.get_by_title(s, "no such title") == []


def test_get_papers_by_author(seeded_engine: Engine) -> None:
    repo = AuthorRepository()
    with with_session(seeded_engine) as s:
        rows = repo.get_papers_by_author(s, "Pino, J.")
    titles = {r.title for r in rows}
    assert "Direct speech-to-speech translation with discrete units" in titles
    assert "Large-scale Self- an Semi-Supervised learning for speech translation" in titles


def test_add_paper_and_links(migrated_engine: Engine) -> None:
    papers = PaperRepository()
    authors = AuthorRepository()
    bib = BibRepository()
    new = PaperCreate(
        title="Brand New",
        summary="A new paper.",
        bibtex_id="New2026",
        bibtex="@misc{New2026, title={Brand New}}",
        authors=["Solo, Han"],
    )
    with with_session(migrated_engine) as s:
        bib.add(s, new.bibtex_id, new.bibtex)
        pid = papers.add(s, new)
        authors.link(s, "Solo, Han", pid)
    with with_session(migrated_engine) as s:
        rows = papers.get_by_title(s, "Brand New")
    assert rows[0].authors == ["Solo, Han"]


def test_unlink_removes_orphan_author(seeded_engine: Engine) -> None:
    papers = PaperRepository()
    authors = AuthorRepository()
    with with_session(seeded_engine) as s:
        pid = papers.get_by_title(s, "Shared Title")[0].paper_id
    with with_session(seeded_engine) as s:
        authors.unlink_all_for_paper(s, pid)
    with with_session(seeded_engine) as s:
        # Alpha, Anne had only that paper -> should be gone
        assert authors.get_papers_by_author(s, "Alpha, Anne") == []


def test_update_bib_rejects_duplicate(seeded_engine: Engine) -> None:
    bib = BibRepository()
    # Try to set Wang's bibtex to Lee's exact bibtex source -> duplicate
    lee_bibtex = next(p.bibtex for p in _seed() if p.bibtex_id == "Lee2022DirectSpeech")
    try:
        with with_session(seeded_engine) as s:
            bib.update(s, "Wang2021LargeScaleSA", lee_bibtex)
        raised = False
    except ValueError:
        raised = True
    assert raised


def _seed() -> list[PaperCreate]:
    from seed_papers import SEED_PAPERS

    return SEED_PAPERS
