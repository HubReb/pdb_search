"""Benchmark-specific fixtures.

Re-uses the ephemeral DB from the main conftest and seeds SEED_PAPERS
before benchmark runs. No personal database required.
"""

from __future__ import annotations

import pytest


@pytest.fixture(scope="module")
def bench_db_url(migrated_db_url: str) -> str:
    """Seeded ephemeral DB URL for benchmark tests.

    Seeds SEED_PAPERS once per module and cleans up afterwards.

    :param migrated_db_url: migrated ephemeral DB URL from session-level fixture
    :return: DB URL with seed data loaded
    """
    from paper_sorts.db.repositories import PaperRepository
    from paper_sorts.db.session import with_session
    from tests.fixtures.seed_papers import SEED_PAPERS

    inserted_ids: list[str] = []
    with with_session(migrated_db_url) as session:
        repo = PaperRepository(session)
        for paper in SEED_PAPERS:
            try:
                result = repo.create(paper)
                inserted_ids.append(result.bibtex_id)
            except ValueError:
                inserted_ids.append(paper.bibtex_id)

    yield migrated_db_url  # type: ignore[misc]

    with with_session(migrated_db_url) as session:
        repo = PaperRepository(session)
        for bib_id in inserted_ids:
            try:
                repo.delete(bib_id)
            except KeyError:
                pass
