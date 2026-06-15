"""Integration tests for the repository layer against a real PostgreSQL.

Asserted rows trace back to ``tests/fixtures/seed_papers.SEED_PAPERS``
(Constitution Principle II — no mocking, seed co-located).
"""

from __future__ import annotations

import pytest
from sqlalchemy.orm import Session

from paper_sorts.db.repositories import (
    DuplicateBibtexKeyError,
    PaperCreate,
    PaperRepository,
)


def test_search_by_title_single_match(seeded_session: Session) -> None:
    """A unique title returns one summary with joined authors and bib."""
    repo = PaperRepository(seeded_session)
    results = repo.search_by_title("Direct speech-to-speech translation with discrete units")
    assert len(results) == 1
    summary = results[0]
    assert summary.authors == ["Lee, Ann", "Chen, Peng-Jen", "Pino, J."]
    assert summary.bibtex_id == "Lee2022Direct"
    assert "discrete units" in summary.bibtex


def test_search_by_title_multiple_matches(seeded_session: Session) -> None:
    """A shared title returns one summary per paper (disambiguation source)."""
    repo = PaperRepository(seeded_session)
    results = repo.search_by_title("On Calibration")
    assert len(results) == 2
    keys = {r.bibtex_id for r in results}
    assert keys == {"Mueller2020Calibration", "Smith2021Calibration"}


def test_search_by_title_not_found(seeded_session: Session) -> None:
    """An unknown title yields no results."""
    assert PaperRepository(seeded_session).search_by_title("no such title") == []


def test_search_by_author_multiple_papers(seeded_session: Session) -> None:
    """An author on two papers returns both."""
    repo = PaperRepository(seeded_session)
    results = repo.search_by_author("Pino, J.")
    titles = {r.title for r in results}
    assert titles == {
        "Direct speech-to-speech translation with discrete units",
        "Large-scale Self- and Semi-Supervised learning for speech translation",
    }


def test_bibtex_accents_round_trip(seeded_session: Session) -> None:
    """LaTeX accents/escapes survive storage and retrieval."""
    repo = PaperRepository(seeded_session)
    summary = repo.search_by_title("On Calibration")
    mueller = next(s for s in summary if s.bibtex_id == "Mueller2020Calibration")
    assert '\\"o' in mueller.bibtex
    assert "\\&" in mueller.bibtex


def test_add_and_retrieve(seeded_session: Session) -> None:
    """A newly added paper is retrievable by both title and author."""
    repo = PaperRepository(seeded_session)
    repo.add(
        PaperCreate(
            title="A Brand New Paper",
            authors=["Newton, Isaac"],
            summary="Something new.",
            bibtex_id="Newton1687",
            bibtex="@book{Newton1687, title={Principia}}",
        )
    )
    seeded_session.flush()
    assert repo.search_by_title("A Brand New Paper")[0].authors == ["Newton, Isaac"]
    assert repo.search_by_author("Newton, Isaac")[0].title == "A Brand New Paper"


def test_add_duplicate_key_rejected(seeded_session: Session) -> None:
    """Re-adding an existing BibTeX key raises."""
    repo = PaperRepository(seeded_session)
    with pytest.raises(DuplicateBibtexKeyError):
        repo.add(
            PaperCreate(
                title="Dup",
                authors=["X, Y"],
                summary="dup",
                bibtex_id="Lee2022Direct",
                bibtex="@misc{dup}",
            )
        )


def test_update_title_and_contents(seeded_session: Session) -> None:
    """Title and contents updates persist."""
    repo = PaperRepository(seeded_session)
    paper = repo.search_by_title("Direct speech-to-speech translation with discrete units")[0]
    # locate id via author search join is awkward; add a fresh paper to own its id
    new_id = repo.add(
        PaperCreate(
            title="Editable",
            authors=["Ed, Itor"],
            summary="before",
            bibtex_id="Edit1",
            bibtex="@misc{Edit1}",
        )
    )
    seeded_session.flush()
    repo.update_title(new_id, "Edited Title")
    repo.update_contents(new_id, "after")
    seeded_session.flush()
    updated = repo.search_by_title("Edited Title")[0]
    assert updated.summary == "after"
    assert paper.bibtex_id == "Lee2022Direct"  # untouched


def test_delete_removes_paper_and_orphan_authors(seeded_session: Session) -> None:
    """Deleting a paper removes its links, orphaned authors, and bib row."""
    repo = PaperRepository(seeded_session)
    summary = repo.search_by_title(
        "Large-scale Self- and Semi-Supervised learning for speech translation"
    )[0]
    repo.delete(summary)
    seeded_session.flush()
    assert repo.search_by_title(summary.title) == []
    # "Wang, Changhan" was only on this paper -> orphan removed.
    assert repo.search_by_author("Wang, Changhan") == []
    # "Pino, J." remains (still on the Direct speech paper).
    assert repo.search_by_author("Pino, J.")
