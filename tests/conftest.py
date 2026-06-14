"""Pytest session fixtures for paper_sorts integration tests.

Provides:
    postgresql_proc: Session-scoped ephemeral PostgreSQL process (pytest-postgresql).
    ephemeral_db_url: SQLAlchemy-compatible URL for the ephemeral database,
        with the test database already created and schema applied.
    db_session: Session-scoped SQLAlchemy Session pre-loaded with the Alembic schema.
    clean_db_session: Function-scoped Session that rolls back after each test.
    seeded_session: Function-scoped Session with SEED_PAPERS already inserted
        (flushed within a savepoint; visible in the same session only).

Architecture note on isolation:
    Tests that call paper_service.* (which opens independent sessions via
    with_session()) need data that is COMMITTED to the DB, not just flushed in
    a savepoint. Such tests must use ``ephemeral_db_url`` directly and manage
    their own data setup via the service layer — they MUST NOT rely on
    ``seeded_session`` for data visibility across sessions.

    Tests that use repositories directly (same session object) CAN use
    ``seeded_session`` / ``clean_db_session``.

All integration tests against the persistence layer MUST use these fixtures.
No mocking of the SQLAlchemy session or DB driver is permitted
(constitution Principle II).
"""

from __future__ import annotations

import os
import pathlib
from collections.abc import Generator

import pytest
from pytest_postgresql import factories
from pytest_postgresql.janitor import DatabaseJanitor
from sqlalchemy.orm import Session

# Use the system pg_ctl at /usr/bin/pg_ctl (PostgreSQL 18)
postgresql_proc = factories.postgresql_proc(executable="/usr/bin/pg_ctl")


def _project_root() -> pathlib.Path:
    """Return the absolute path to the repository root."""
    return pathlib.Path(__file__).parent.parent


@pytest.fixture(scope="session")
def ephemeral_db_url(postgresql_proc: object) -> Generator[str, None, None]:  # type: ignore[type-arg]
    """Create the test database and return a SQLAlchemy-compatible URL.

    Uses DatabaseJanitor to create and tear down the ephemeral database for
    the test session duration.

    Args:
        postgresql_proc: The running ephemeral PostgreSQL process fixture.

    Yields:
        A SQLAlchemy connection URL string (postgresql+psycopg://...).
    """
    proc = postgresql_proc  # type: ignore[attr-defined]
    dbname = proc.dbname

    with DatabaseJanitor(
        user=proc.user,
        host=proc.host,
        port=proc.port,
        dbname=dbname,
        version=proc.version,
        password=proc.password,
    ):
        url = f"postgresql+psycopg://{proc.user}:@{proc.host}:{proc.port}/{dbname}"
        yield url


@pytest.fixture(scope="session")
def db_session(ephemeral_db_url: str) -> Generator[Session, None, None]:
    """Apply Alembic migrations and yield a SQLAlchemy Session.

    Runs ``alembic upgrade head`` against the ephemeral database URL, then
    yields a session. The schema persists for the duration of the test session
    and is torn down automatically when the ephemeral process exits.

    Args:
        ephemeral_db_url: SQLAlchemy URL for the ephemeral database.

    Yields:
        An active SQLAlchemy Session bound to the ephemeral database.
    """
    import alembic.command
    import alembic.config

    # Point Alembic at the ephemeral DB
    os.environ["PDBSEARCH_DATABASE_URL"] = ephemeral_db_url

    alembic_cfg = alembic.config.Config(
        str(_project_root() / "alembic.ini")
    )
    alembic_cfg.set_main_option("sqlalchemy.url", ephemeral_db_url)
    alembic.command.upgrade(alembic_cfg, "head")

    from paper_sorts.db.session import get_engine

    engine = get_engine(ephemeral_db_url)
    with Session(engine) as session:
        yield session
    engine.dispose()


@pytest.fixture
def clean_db_session(db_session: Session) -> Generator[Session, None, None]:
    """Yield a session and roll back all changes after each test.

    Wraps each test in a savepoint so modifications are not visible to other
    tests in the session. Use this fixture for repository-layer tests that
    share the same session object.

    NOTE: Data flushed in this session is NOT visible to code that opens an
    independent session (e.g., paper_service.*). See module docstring.

    Args:
        db_session: The session-scoped Session from db_session fixture.

    Yields:
        The same Session, with automatic rollback on test completion.
    """
    db_session.begin_nested()
    yield db_session
    db_session.rollback()


@pytest.fixture
def seeded_session(clean_db_session: Session) -> Session:
    """Return a Session pre-populated with SEED_PAPERS.

    Seeds the database with the canonical fixture data and returns the session
    for test assertions. Data is flushed (visible in same session) but not
    committed (not visible to independent sessions).

    Use this fixture for repository-layer tests only. Service-layer tests
    that call paper_service.* MUST seed via the service layer itself.

    Args:
        clean_db_session: A clean, rolled-back-after-test Session.

    Returns:
        Session with SEED_PAPERS inserted and flushed.
    """
    from paper_sorts.db.repositories import PaperRepository
    from tests.fixtures.seed_papers import SEED_PAPERS

    for paper in SEED_PAPERS:
        PaperRepository.add(clean_db_session, paper)
    clean_db_session.flush()
    return clean_db_session


@pytest.fixture
def seeded_db_url(ephemeral_db_url: str, db_session: Session) -> Generator[str, None, None]:
    """Yield the ephemeral DB URL after seeding it with SEED_PAPERS (committed).

    This fixture inserts SEED_PAPERS with committed transactions so the data
    is visible to independent sessions (e.g., paper_service.* calls).
    After the test, seed data is deleted via the service layer.

    Use this fixture for service-layer and CLI tests that need seed data
    visible across session boundaries.

    Args:
        ephemeral_db_url: The ephemeral database URL.
        db_session: The session-scoped Session (used to ensure migrations ran).

    Yields:
        The same ephemeral_db_url, after seed data is committed.
    """
    from paper_sorts.db.repositories import PaperRepository
    from paper_sorts.db.session import with_session
    from tests.fixtures.seed_papers import SEED_PAPERS

    # Insert seed data in committed transactions
    paper_ids: list[int] = []
    for paper in SEED_PAPERS:
        try:
            with with_session(ephemeral_db_url) as session:
                pid = PaperRepository.add(session, paper)
                paper_ids.append(pid)
        except ValueError:
            # Already seeded (idempotent)
            pass

    yield ephemeral_db_url

    # Cleanup: remove seeded papers
    from paper_sorts.services.paper_service import delete_paper
    for pid in paper_ids:
        try:
            delete_paper(ephemeral_db_url, pid)
        except ValueError:
            pass
