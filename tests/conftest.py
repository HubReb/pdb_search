"""Pytest fixtures for paper-sorts test suite.

Provides an ephemeral PostgreSQL instance per test session via
``pytest-postgresql``.  No personal database, no ``database.crypt``,
no ``key`` file required.

Fixtures
--------
``postgresql_proc`` — pytest-postgresql process fixture (session-scoped).
``ephemeral_db_url`` — SQLAlchemy DSN for the ephemeral cluster (DB created).
``migrated_db_url`` — DSN after alembic upgrade head applied.
``db_session`` — Session yielded per-test against the migrated DB.
``seeded_session`` — Session with ``SEED_PAPERS`` already inserted.
"""

from __future__ import annotations

import os
import pathlib
from collections.abc import Generator

import pytest
from pytest_postgresql import factories
from pytest_postgresql.janitor import DatabaseJanitor
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from paper_sorts.db.repositories import BibRepository, PaperRepository
from paper_sorts.db.session import with_session
from tests.fixtures.seed_papers import SEED_PAPERS

# ---------------------------------------------------------------------------
# Ephemeral PostgreSQL process (session-scoped, shared across all tests)
# ---------------------------------------------------------------------------

postgresql_proc = factories.postgresql_proc(
    executable="/usr/bin/pg_ctl",
    host="127.0.0.1",
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def ephemeral_db_url(postgresql_proc: object) -> Generator[str, None, None]:  # type: ignore[misc]
    """Create the test database and yield the SQLAlchemy DSN.

    Uses :class:`pytest_postgresql.janitor.DatabaseJanitor` to create and
    drop the test database around the test session.

    :param postgresql_proc: pytest-postgresql process fixture.
    :yields: Full SQLAlchemy DSN string.
    """
    proc = postgresql_proc  # type: ignore[attr-defined]
    dbname = "paper_sorts_test"
    password_part = f":{proc.password}" if proc.password else ""
    with DatabaseJanitor(
        user=proc.user,
        host=proc.host,
        port=proc.port,
        dbname=dbname,
        version=proc.version,
        password=proc.password or None,
    ):
        dsn = f"postgresql+psycopg://{proc.user}{password_part}@{proc.host}:{proc.port}/{dbname}"
        yield dsn


@pytest.fixture(scope="session")
def migrated_db_url(ephemeral_db_url: str) -> str:
    """Run Alembic migrations against the ephemeral DB and return the URL.

    :param ephemeral_db_url: SQLAlchemy DSN from :fixture:`ephemeral_db_url`.
    :returns: The same DSN after migrations have been applied.
    """
    from alembic import command
    from alembic.config import Config

    os.environ["PDBSEARCH_DATABASE_URL"] = ephemeral_db_url
    alembic_ini = str(pathlib.Path(__file__).parent.parent / "alembic.ini")
    cfg = Config(alembic_ini)
    cfg.set_main_option("sqlalchemy.url", ephemeral_db_url)
    command.upgrade(cfg, "head")
    return ephemeral_db_url


@pytest.fixture
def db_session(migrated_db_url: str) -> Generator[Session, None, None]:
    """Yield a SQLAlchemy session against the migrated ephemeral DB.

    Each test gets a fresh session; the session is rolled back and closed
    after the test to avoid state leakage between tests.

    :param migrated_db_url: URL from :fixture:`migrated_db_url`.
    :yields: Open :class:`sqlalchemy.orm.Session`.
    """
    engine = create_engine(migrated_db_url)
    session = Session(engine)
    try:
        yield session
        session.rollback()
    finally:
        session.close()
        engine.dispose()


def _cleanup_paper_and_bib(db_url: str, paper_ids: list[int]) -> None:
    """Delete papers and their associated bib rows from the DB.

    Used by seed fixtures to clean up after each test so the next test can
    re-insert the same seed data without hitting duplicate-key errors.

    :param db_url: SQLAlchemy DSN.
    :param paper_ids: List of ``papers.id`` values to delete.
    """
    with with_session(db_url) as s:
        for paper_id in paper_ids:
            try:
                paper = PaperRepository.get_by_id(s, paper_id)
                bibtex_key = paper.bibtex_key if paper else None
                PaperRepository.delete(s, paper_id)
                if bibtex_key:
                    bib = BibRepository.get_by_key(s, bibtex_key)
                    if bib:
                        s.delete(bib)
                        s.flush()
            except ValueError:
                pass  # already deleted by test


@pytest.fixture
def seeded_session(migrated_db_url: str) -> Generator[Session, None, None]:
    """Yield a session after inserting all ``SEED_PAPERS`` into the DB.

    Inserts seed data in a separate session (committed), then yields a fresh
    session for the test.  After the test, all seed data is deleted to keep
    the DB clean for the next test.

    :param migrated_db_url: URL from :fixture:`migrated_db_url`.
    :yields: Open :class:`sqlalchemy.orm.Session` with seed data present.
    """
    inserted_ids: list[int] = []

    with with_session(migrated_db_url) as s:
        for paper_data in SEED_PAPERS:
            summary = PaperRepository.create(s, paper_data)
            inserted_ids.append(summary.paper_id)

    engine = create_engine(migrated_db_url)
    session = Session(engine)
    try:
        yield session
        session.rollback()
    finally:
        session.close()
        engine.dispose()

    _cleanup_paper_and_bib(migrated_db_url, inserted_ids)
