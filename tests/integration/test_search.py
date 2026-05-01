"""Integration tests for ``PaperService.search_by_*`` (T032).

Hits the seeded ephemeral DB through the full repository surface — no
mocking. The seed dataset (``tests/fixtures/seed_papers.SEED_PAPERS``)
includes two papers titled ``"On Fairness in Machine Translation"``
specifically so the multi-match path is exercised, plus
``"Pino, J."`` appearing on two papers for the same reason on the
author axis.
"""

from __future__ import annotations

import pytest
from sqlalchemy.orm import Session

from paper_sorts.services.paper_service import PaperService


def test_search_by_title_single_match(db_session: Session) -> None:
    service = PaperService(db_session)
    results = service.search_by_title(
        "Large-scale Self- and Semi-Supervised learning for speech translation"
    )
    assert len(results) == 1
    paper = results[0]
    assert paper.bibtex_id == "Wang2021LargeScaleSA"
    assert paper.bibtex is not None
    assert "Wang2021LargeScaleSA" in paper.bibtex
    assert set(paper.authors) == {"Wang, C.", "Pino, J."}


def test_search_by_title_multi_match(db_session: Session) -> None:
    """Two seeded papers share the title ``On Fairness in Machine Translation``."""
    service = PaperService(db_session)
    results = service.search_by_title("On Fairness in Machine Translation")
    assert len(results) == 2
    bibtex_ids = {p.bibtex_id for p in results}
    assert bibtex_ids == {"Schoettler2023FairnessMT", "Lee2024FairnessMT"}


def test_search_by_title_no_match(db_session: Session) -> None:
    service = PaperService(db_session)
    results = service.search_by_title("This Paper Does Not Exist")
    assert results == []


def test_search_by_author_single_match(db_session: Session) -> None:
    """Schöttler appears on exactly one seeded paper."""
    service = PaperService(db_session)
    results = service.search_by_author("Schöttler, K.")
    assert len(results) == 1
    assert results[0].bibtex_id == "Schoettler2023FairnessMT"


def test_search_by_author_multi_match(db_session: Session) -> None:
    """Pino, J. appears on two seeded papers."""
    service = PaperService(db_session)
    results = service.search_by_author("Pino, J.")
    assert len(results) == 2
    bibtex_ids = {p.bibtex_id for p in results}
    assert bibtex_ids == {"Lee2022DirectSpeechToSpeech", "Wang2021LargeScaleSA"}


def test_search_by_author_no_match(db_session: Session) -> None:
    service = PaperService(db_session)
    results = service.search_by_author("Nobody, A.")
    assert results == []


def test_search_isolation_between_tests(db_session: Session) -> None:
    """The savepoint-rollback fixture must restore the seed between tests.

    This test verifies the harness contract: even if a previous test
    inserted, updated, or deleted rows, the seeded count is intact.
    """
    service = PaperService(db_session)
    pino_papers = service.search_by_author("Pino, J.")
    fairness_papers = service.search_by_title("On Fairness in Machine Translation")
    assert len(pino_papers) == 2
    assert len(fairness_papers) == 2


@pytest.mark.parametrize(
    ("title", "expected_count"),
    [
        ("Direct speech-to-speech translation with discrete units", 1),
        ("Large-scale Self- and Semi-Supervised learning for speech translation", 1),
        ("On Fairness in Machine Translation", 2),
    ],
)
def test_search_by_title_seeded_titles(
    db_session: Session, title: str, expected_count: int
) -> None:
    service = PaperService(db_session)
    assert len(service.search_by_title(title)) == expected_count
