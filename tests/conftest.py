"""pytest session fixtures for paper_sorts integration tests.

Provides:
  - postgresql_proc_fixture: session-scoped ephemeral PostgreSQL process
  - ephemeral_db_url: SQLAlchemy URL for the ephemeral database
  - db_session: session-scoped SQLAlchemy Session with migrations applied
    and SEED_PAPERS loaded

No personal database, no database.crypt, no key file required
(constitution Principle II / US3).
"""

from __future__ import annotations

import os

import pytest
from pytest_postgresql.factories import postgresql_proc
from pytest_postgresql.janitor import DatabaseJanitor
from sqlalchemy.orm import Session

# Ephemeral PostgreSQL process (session-scoped, auto-assigned port).
postgresql_proc_fixture = postgresql_proc(
    executable="/usr/bin/pg_ctl",
    host="127.0.0.1",
    port=None,  # auto-assign available port
)


@pytest.fixture(scope="session")
def ephemeral_db_url(postgresql_proc_fixture) -> str:  # type: ignore[no-untyped-def]
    """Create an ephemeral test database and return its SQLAlchemy URL.

    Uses DatabaseJanitor to create and drop the DB around the test session.
    The database is 'paper_sorts_test' on the ephemeral PostgreSQL process.

    :param postgresql_proc_fixture: Session-scoped PostgreSQL process fixture.
    :yields: SQLAlchemy URL string for the test database.
    """
    proc = postgresql_proc_fixture
    dbname = "paper_sorts_test"

    janitor = DatabaseJanitor(
        user=proc.user,
        host=proc.host,
        port=proc.port,
        dbname=dbname,
        version=proc.version,
        password=proc.password,
    )
    with janitor:
        url = f"postgresql+psycopg://{proc.user}@{proc.host}:{proc.port}/{dbname}"
        yield url


@pytest.fixture(scope="session")
def db_session(ephemeral_db_url: str) -> Session:  # type: ignore[return]
    """Session-scoped SQLAlchemy Session with migrations applied and seed data loaded.

    Applies all Alembic migrations, then inserts SEED_PAPERS into the database.
    The session is yielded for read access; write tests use ephemeral_db_url directly.

    :param ephemeral_db_url: URL from ephemeral_db_url fixture.
    :yields: Active SQLAlchemy Session with seed data.
    """
    from paper_sorts.cli.migrate import run_migrate
    from paper_sorts.db.repositories import PaperRepository
    from paper_sorts.db.session import get_engine, with_session
    from tests.fixtures.seed_papers import SEED_PAPERS

    # Set env var so alembic env.py picks up the test DB URL
    os.environ["PDBSEARCH_DATABASE_URL"] = ephemeral_db_url

    # Apply migrations
    run_migrate(ephemeral_db_url)

    # Seed the database
    import contextlib

    with with_session(ephemeral_db_url) as session:
        repo = PaperRepository(session)
        for paper in SEED_PAPERS:
            with contextlib.suppress(ValueError):  # already seeded = idempotent
                repo.add(paper)

    # Provide a long-lived session for fixture-based reads
    engine = get_engine(ephemeral_db_url)
    session = Session(engine)
    yield session
    session.close()
    engine.dispose()
