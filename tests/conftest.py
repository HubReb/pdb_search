"""pytest fixtures for paper_sorts tests.

Provides ephemeral PostgreSQL per test session using pytest-postgresql.
No personal database, no database.crypt, no key file required.

Fixtures:
- postgresql_proc: pytest-postgresql process fixture (session-scoped)
- ephemeral_db_url: SQLAlchemy DSN for the ephemeral test database
- raw_engine: Engine connected to the ephemeral test database with schema applied
- seeded_engine: Engine with SEED_PAPERS data pre-inserted
"""

from __future__ import annotations

import pytest
from pytest_postgresql import factories
from sqlalchemy import Engine

from paper_sorts.db.models import Base
from paper_sorts.db.repositories import PaperCreate, PaperRepository
from paper_sorts.db.session import get_engine, with_session
from tests.fixtures.seed_papers import SEED_PAPERS

# Use the system pg_ctl at /usr/bin/pg_ctl (PostgreSQL 18 on this host)
postgresql_proc = factories.postgresql_proc(
    executable="/usr/bin/pg_ctl",
    host="127.0.0.1",
    port=None,  # auto-assign
)

postgresql = factories.postgresql("postgresql_proc")


@pytest.fixture(scope="session")
def ephemeral_db_url(postgresql_proc: object) -> str:  # type: ignore[type-arg]
    """Return a SQLAlchemy DSN for the ephemeral test PostgreSQL instance.

    :param postgresql_proc: pytest-postgresql process fixture
    :return: PostgreSQL DSN string
    :rtype: str
    """
    # postgresql_proc exposes the connection info
    proc = postgresql_proc  # type: ignore[assignment]
    host = getattr(proc, "host", "127.0.0.1")
    port = getattr(proc, "port", 5432)
    user = getattr(proc, "user", "postgres")
    dbname = "test_paper_sorts"
    return f"postgresql+psycopg://{user}@{host}:{port}/{dbname}"


@pytest.fixture(scope="session")
def raw_engine(postgresql_proc: object) -> Engine:  # type: ignore[type-arg]
    """Provide a SQLAlchemy Engine with the canonical schema applied.

    Creates all tables via SQLAlchemy ORM metadata (not Alembic) for speed.
    The schema matches what Alembic migration 001 would produce.

    :param postgresql_proc: pytest-postgresql process fixture
    :return: configured SQLAlchemy Engine
    :rtype: Engine
    """
    proc = postgresql_proc  # type: ignore[assignment]
    host = getattr(proc, "host", "127.0.0.1")
    port = getattr(proc, "port", 5432)
    user = getattr(proc, "user", "postgres")

    # Create the test database
    import psycopg  # type: ignore[import]

    dsn = f"host={host} port={port} user={user} dbname=postgres"
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("DROP DATABASE IF EXISTS test_paper_sorts")
        conn.execute("CREATE DATABASE test_paper_sorts")

    db_url = f"postgresql+psycopg://{user}@{host}:{port}/test_paper_sorts"
    engine = get_engine(db_url)
    Base.metadata.create_all(engine)
    return engine


@pytest.fixture(scope="function")
def clean_engine(raw_engine: Engine) -> Engine:
    """Provide a raw_engine with tables truncated between tests.

    :param raw_engine: session-scoped engine with schema
    :return: Engine with empty tables
    :rtype: Engine
    """
    with with_session(raw_engine) as session:
        # Truncate in FK-safe order
        session.execute(  # type: ignore[call-overload]
            __import__("sqlalchemy").text(
                "TRUNCATE TABLE authors_papers, authors_id, papers, bib RESTART IDENTITY CASCADE"
            )
        )
    return raw_engine


@pytest.fixture(scope="function")
def seeded_engine(clean_engine: Engine) -> Engine:
    """Provide a clean_engine with SEED_PAPERS pre-inserted.

    :param clean_engine: engine with empty tables
    :return: Engine with seed data
    :rtype: Engine
    """
    with with_session(clean_engine) as session:
        repo = PaperRepository(session)
        for paper in SEED_PAPERS:
            repo.create(paper)
    return clean_engine


@pytest.fixture(scope="function")
def seed_papers() -> list[PaperCreate]:
    """Return the canonical SEED_PAPERS list for use in assertions.

    :return: list of PaperCreate objects that were seeded
    :rtype: list[PaperCreate]
    """
    return list(SEED_PAPERS)
