"""Shared pytest fixtures for paper_sorts tests.

Provides an ephemeral PostgreSQL instance per test session via
``pytest-postgresql``.  The host's ``pg_ctl`` at ``/usr/bin/pg_ctl`` is used
to spin up and tear down the cluster.  No personal database or credentials are
required.

Session-scoped fixtures:
- ``postgresql_proc``: manages the PostgreSQL process lifecycle.
- ``ephemeral_db_url``: creates a fresh database, runs Alembic migrations to
  ``head``, yields the SQLAlchemy URL, then drops the database.

Function-scoped fixture:
- ``db_engine``: creates a fresh SQLAlchemy engine for each test; seeds the
  database from :data:`~tests.fixtures.seed_papers.SEED_PAPERS`.
"""

from __future__ import annotations

import pytest
from pytest_postgresql import factories
from sqlalchemy import text

from paper_sorts.db.repositories import PaperRepository
from paper_sorts.db.session import get_engine, with_session

# ---------------------------------------------------------------------------
# Process fixture — one PG process for the whole test session
# ---------------------------------------------------------------------------

postgresql_proc = factories.postgresql_proc(
    executable="/usr/bin/pg_ctl",
    port=None,   # auto-assign a free port
)


# ---------------------------------------------------------------------------
# Session-scoped database URL
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def ephemeral_db_url(postgresql_proc: factories.PostgreSQLExecutor) -> str:  # type: ignore[type-arg]
    """Yield a SQLAlchemy URL pointing at a freshly-migrated ephemeral database.

    Runs Alembic migrations to ``head`` before yielding so the schema is ready.
    The URL is valid for the duration of the test session.

    :param postgresql_proc: pytest-postgresql process fixture.
    :yields: SQLAlchemy connection URL string.
    """
    host = postgresql_proc.host
    port = postgresql_proc.port
    user = postgresql_proc.user
    dbname = "test_paper_sorts"

    # Create the test database using the default superuser DB
    admin_url = f"postgresql+psycopg://{user}@{host}:{port}/postgres"
    admin_engine = get_engine(admin_url)
    with admin_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f'CREATE DATABASE "{dbname}"'))
    admin_engine.dispose()

    db_url = f"postgresql+psycopg://{user}@{host}:{port}/{dbname}"

    # Run Alembic migrations
    import os

    os.environ["PDBSEARCH_DATABASE_URL"] = db_url
    from alembic import command
    from alembic.config import Config as AlembicConfig

    alembic_cfg = AlembicConfig("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", db_url)
    command.upgrade(alembic_cfg, "head")

    yield db_url

    # Teardown
    admin_engine2 = get_engine(admin_url)
    with admin_engine2.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f'DROP DATABASE IF EXISTS "{dbname}"'))
    admin_engine2.dispose()


# ---------------------------------------------------------------------------
# Function-scoped engine + seed data
# ---------------------------------------------------------------------------


@pytest.fixture
def db_engine(ephemeral_db_url: str) -> sqlalchemy.Engine:  # type: ignore[name-defined]  # noqa: F821
    """Yield a SQLAlchemy Engine; seed the DB, then clean up after each test.

    The seed data is inserted before each test and all rows are deleted after
    the test completes, so tests are isolated from one another.

    :param ephemeral_db_url: Session-scoped URL from :func:`ephemeral_db_url`.
    :yields: A :class:`sqlalchemy.Engine` connected to the test database.
    """
    from tests.fixtures.seed_papers import SEED_PAPERS

    engine = get_engine(ephemeral_db_url)

    # Seed
    with with_session(engine) as session:
        for paper in SEED_PAPERS:
            try:
                PaperRepository.add_paper(session, paper)
            except ValueError:
                pass  # skip if already seeded from a prior partial run

    yield engine

    # Cleanup — delete in reverse FK order
    with with_session(engine) as session:
        from sqlalchemy import delete

        from paper_sorts.db.models import Author, AuthorPaper, Bib, Paper

        session.execute(delete(AuthorPaper))
        session.execute(delete(Author))
        session.execute(delete(Paper))
        session.execute(delete(Bib))

    engine.dispose()
