"""pytest fixtures for paper_sorts integration tests.

Provides:
    ``postgresql_proc`` — an ephemeral PostgreSQL process per session.
    ``ephemeral_db_url`` — a DSN string pointing to the ephemeral DB with
        Alembic migrations applied (all tests share the schema, each gets
        a fresh session).
    ``seeded_engine`` — engine pointing at a DB seeded with SEED_PAPERS.
"""

from __future__ import annotations

import pytest
from pytest_postgresql import factories as pg_factories
from sqlalchemy import text

from paper_sorts.db.session import get_engine, with_session

# Use the host pg_ctl at /usr/bin/pg_ctl
postgresql_proc = pg_factories.postgresql_proc(
    executable="/usr/bin/pg_ctl",
    host="127.0.0.1",
)

# A pytest-postgresql fixture that creates a database using our process fixture
postgresql = pg_factories.postgresql("postgresql_proc")


@pytest.fixture(scope="session")
def ephemeral_db_url(postgresql_proc: object) -> str:  # type: ignore[type-arg]
    """Return a PostgreSQL DSN for the ephemeral test database.

    Applies all Alembic migrations to the ephemeral DB before yielding.
    The DSN uses the psycopg (v3) driver.

    :param postgresql_proc: pytest-postgresql process fixture.
    :return: DSN string for the migrated ephemeral database.
    """
    import os
    import subprocess
    import sys

    proc = postgresql_proc  # type: ignore[attr-defined]

    user = proc.user
    host = proc.host
    port = proc.port
    dbname = "test_paper_sorts"

    # Build the DSN
    dsn = f"postgresql+psycopg://{user}@{host}:{port}/{dbname}"

    # Create the test database (must use AUTOCOMMIT — CREATE DATABASE
    # cannot run inside a transaction block)
    admin_dsn = f"postgresql+psycopg://{user}@{host}:{port}/postgres"
    admin_engine = get_engine(admin_dsn)
    with admin_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f'CREATE DATABASE "{dbname}"'))
    admin_engine.dispose()

    # Run Alembic migrations
    env = {**os.environ, "PDBSEARCH_DATABASE_URL": dsn}
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        env=env,
        capture_output=True,
        text=True,
        cwd=str(
            __file__
        ).replace("tests/conftest.py", "").rstrip("/") or ".",
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Alembic migration failed:\n{result.stdout}\n{result.stderr}"
        )

    return dsn


@pytest.fixture
def engine(ephemeral_db_url: str):  # type: ignore[type-arg]
    """Return a SQLAlchemy engine for the migrated ephemeral DB.

    :param ephemeral_db_url: DSN from the :func:`ephemeral_db_url` fixture.
    :return: SQLAlchemy engine.
    """
    eng = get_engine(ephemeral_db_url)
    yield eng
    eng.dispose()


@pytest.fixture
def seeded_engine(engine):  # type: ignore[type-arg]
    """Engine pointing at the ephemeral DB, pre-seeded with SEED_PAPERS.

    Seed rows are inserted in a transaction that is rolled back after each
    test to keep tests independent.

    :param engine: Engine from the :func:`engine` fixture.
    :return: Engine (same object; seeding is done in a nested transaction).
    """
    from paper_sorts.db.repositories import PaperRepository
    from tests.fixtures.seed_papers import SEED_PAPERS

    repo = PaperRepository()
    paper_ids: list[int] = []

    with with_session(engine) as session:
        for paper in SEED_PAPERS:
            result = repo.add_paper(session, paper)
            paper_ids.append(result.id)

    yield engine

    # Cleanup: delete inserted papers
    from paper_sorts.services.paper_service import delete_paper
    for pid in paper_ids:
        try:
            delete_paper(engine, pid)
        except Exception:
            pass
