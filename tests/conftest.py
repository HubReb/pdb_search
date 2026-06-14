"""Pytest configuration and session-scoped fixtures for paper_sorts tests.

Provides:
  postgresql_proc — ephemeral PostgreSQL process (via pytest-postgresql)
  ephemeral_db_url — connection URL for the test DB with migrations applied
  db_url — per-test clean DB URL (tables truncated between tests)
  seeded_db_url — per-test DB URL pre-populated with SEED_PAPERS

No developer-local database, no database.crypt, no key file required.
(Constitution Principle II — integration tests manage their own ephemeral DB.)
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from pytest_postgresql import factories
from sqlalchemy import create_engine, text

# Use the system pg_ctl (PostgreSQL 18 at /usr/bin/pg_ctl)
postgresql_proc = factories.postgresql_proc(
    host="127.0.0.1",
    executable="/usr/bin/pg_ctl",
)
# This fixture creates the actual database under the running server
postgresql = factories.postgresql("postgresql_proc")


@pytest.fixture(scope="session")
def ephemeral_db_url(postgresql_proc: factories.PostgreSQLExecutor) -> str:  # type: ignore[type-arg]
    """Return a SQLAlchemy URL for an empty but migrated test database.

    Uses postgresql_proc (the cluster) and creates the test DB manually,
    then runs Alembic migrations.

    Args:
        postgresql_proc: The ephemeral PostgreSQL process fixture.

    Returns:
        SQLAlchemy-compatible connection string.
    """
    # Connect to the default 'postgres' database to create our test DB
    admin_url = (
        f"postgresql+psycopg://{postgresql_proc.user}:@"
        f"{postgresql_proc.host}:{postgresql_proc.port}/postgres"
    )
    test_dbname = "paper_sorts_test"
    engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        conn.execute(text(f"DROP DATABASE IF EXISTS {test_dbname}"))
        conn.execute(text(f"CREATE DATABASE {test_dbname}"))
    engine.dispose()

    url = (
        f"postgresql+psycopg://{postgresql_proc.user}:@"
        f"{postgresql_proc.host}:{postgresql_proc.port}/{test_dbname}"
    )

    # Run Alembic migrations programmatically
    os.environ["PDBSEARCH_DATABASE_URL"] = url
    from alembic import command
    from alembic.config import Config

    alembic_cfg = Config("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", url)
    command.upgrade(alembic_cfg, "head")
    return url


@pytest.fixture()
def db_url(ephemeral_db_url: str) -> str:
    """Return the ephemeral DB URL and truncate tables before each test.

    Args:
        ephemeral_db_url: Session-scoped migrated DB URL.

    Returns:
        The same URL after clearing all rows.
    """
    engine = create_engine(ephemeral_db_url)
    with engine.begin() as conn:
        conn.execute(
            text(
                "TRUNCATE TABLE authors_papers, authors_id, papers, bib"
                " RESTART IDENTITY CASCADE"
            )
        )
    engine.dispose()
    return ephemeral_db_url


@pytest.fixture()
def seeded_db_url(db_url: str) -> str:
    """Return a DB URL pre-populated with SEED_PAPERS.

    Args:
        db_url: Clean ephemeral DB URL.

    Returns:
        The same URL after seeding.
    """
    from paper_sorts.db.session import with_session
    from paper_sorts.services.paper_service import add_paper
    from tests.fixtures.seed_papers import SEED_PAPERS

    with with_session(db_url) as session:
        for paper in SEED_PAPERS:
            add_paper(session, paper)
    return db_url
