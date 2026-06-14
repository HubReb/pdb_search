"""Shared pytest fixtures for paper_sorts tests.

Uses pytest-postgresql to provision an ephemeral PostgreSQL cluster via
the host pg_ctl at /usr/bin/pg_ctl. No personal database or credentials required.

Fixtures:
  postgresql_proc: process-level PostgreSQL fixture (from pytest-postgresql)
  postgresql: connection-level fixture providing a psycopg.Connection
  ephemeral_db_url: SQLAlchemy DSN for the ephemeral database
  migrated_db_url: ephemeral DB URL after Alembic migrations applied
  db_url: per-test alias for migrated_db_url (session reset after each test)
  seeded_db_url: migrated DB with SEED_PAPERS pre-inserted
"""

from __future__ import annotations

import pytest
from pytest_postgresql import factories

from paper_sorts.db.session import reset_engine

# Provision ephemeral PostgreSQL using the host pg_ctl
postgresql_proc = factories.postgresql_proc(
    executable="/usr/bin/pg_ctl",
    port=None,  # auto-assign a free port
)
# Connection-level fixture — creates the database and gives a psycopg.Connection
postgresql = factories.postgresql("postgresql_proc")


@pytest.fixture(scope="session")
def ephemeral_db_url(postgresql_proc: object) -> str:  # type: ignore[type-arg]
    """Return a SQLAlchemy DSN for the ephemeral PostgreSQL instance.

    Connects via the postgresql_proc attributes to form the DSN.
    The URL does NOT reference ../../database.crypt or ../../key.

    :param postgresql_proc: process-level pytest-postgresql fixture
    :return: PostgreSQL DSN string suitable for SQLAlchemy
    """
    proc = postgresql_proc
    host = getattr(proc, "host", "127.0.0.1")
    port = getattr(proc, "port", 5432)
    user = getattr(proc, "user", "postgres")
    # The postgresql_proc creates the default user but not a named database;
    # we connect to the default 'postgres' database for migrations.
    url = f"postgresql+psycopg://{user}@{host}:{port}/postgres"
    return url


@pytest.fixture(scope="session")
def migrated_db_url(ephemeral_db_url: str) -> str:
    """Return the ephemeral DB URL after running Alembic migrations.

    :param ephemeral_db_url: DSN for the ephemeral PostgreSQL instance
    :return: same DSN (migrations applied in-place)
    """
    import os

    from alembic import command
    from alembic.config import Config

    os.environ["PDBSEARCH_DATABASE_URL"] = ephemeral_db_url
    alembic_cfg = Config("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", ephemeral_db_url)
    command.upgrade(alembic_cfg, "head")
    return ephemeral_db_url


@pytest.fixture
def db_url(migrated_db_url: str) -> str:  # type: ignore[misc]
    """Per-test fixture providing the migrated ephemeral DB URL.

    Resets the SQLAlchemy engine cache after each test to avoid
    connection leaks between tests.

    :param migrated_db_url: migrated ephemeral DB URL
    :yields: DB URL string
    """
    yield migrated_db_url
    reset_engine()


@pytest.fixture
def seeded_db_url(db_url: str) -> str:  # type: ignore[misc]
    """Ephemeral DB URL with SEED_PAPERS pre-inserted.

    Seeds are inserted within a transaction that is committed before
    yielding, and rows are cleaned up (deleted) after each test to
    preserve test isolation.

    :param db_url: migrated ephemeral DB URL
    :yields: DB URL with seed data available
    """
    from paper_sorts.db.repositories import PaperRepository
    from paper_sorts.db.session import with_session
    from tests.fixtures.seed_papers import SEED_PAPERS

    bibtex_ids: list[str] = []
    with with_session(db_url) as session:
        repo = PaperRepository(session)
        for paper in SEED_PAPERS:
            try:
                result = repo.create(paper)
                bibtex_ids.append(result.bibtex_id)
            except ValueError:
                # Already exists (e.g. from a previous test that didn't clean up)
                bibtex_ids.append(paper.bibtex_id)

    yield db_url

    # Teardown: remove seeded rows
    with with_session(db_url) as session:
        repo = PaperRepository(session)
        for bib_id in bibtex_ids:
            try:
                repo.delete(bib_id)
            except KeyError:
                pass
    reset_engine()
