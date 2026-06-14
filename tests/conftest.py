"""Pytest configuration and fixtures for paper_sorts tests.

Fixtures:
    postgresql_proc — session-scoped ephemeral PostgreSQL process.
    db_engine       — session-scoped SQLAlchemy engine; creates the test DB
                      via DatabaseJanitor and applies ORM schema.
    db_session      — function-scoped Session with per-test rollback isolation
                      using a SAVEPOINT so committed seed data rolls back too.
    seeded_session  — db_session pre-loaded with SEED_PAPERS.

Constitution Principle II:
    Integration tests run against a real PostgreSQL instance provisioned by
    pytest-postgresql using pg_ctl at /usr/bin/pg_ctl.  No mocking of the
    SQLAlchemy session or driver is permitted.
"""

from __future__ import annotations

from collections.abc import Generator

import pytest
from pytest_postgresql import factories
from pytest_postgresql.janitor import DatabaseJanitor
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from paper_sorts.db.models import Base
from paper_sorts.db.repositories import PaperRepository
from tests.fixtures.seed_papers import SEED_PAPERS

# Ephemeral PostgreSQL process fixture — one per test session.
postgresql_proc = factories.postgresql_proc(executable="/usr/bin/pg_ctl")


@pytest.fixture(scope="session")
def db_engine(postgresql_proc: object) -> Generator[object, None, None]:
    """Session-scoped engine; creates the test database and ORM schema.

    :param postgresql_proc: the running PostgreSQL proc fixture.
    :yields: a :class:`sqlalchemy.engine.Engine`.
    """
    proc = postgresql_proc  # type: ignore[attr-defined]
    user: str = proc.user
    host: str = proc.host
    port: int = proc.port
    dbname: str = proc.dbname
    version = proc.version
    template_dbname = getattr(proc, "template_dbname", None)

    with DatabaseJanitor(
        user=user,
        host=host,
        port=port,
        dbname=dbname,
        version=version,
        template_dbname=template_dbname,
    ):
        url = f"postgresql+psycopg://{user}@{host}:{port}/{dbname}"
        engine = create_engine(url)
        Base.metadata.create_all(engine)
        yield engine
        engine.dispose()


@pytest.fixture()
def db_session(db_engine: object) -> Generator[Session, None, None]:
    """Function-scoped Session with full rollback isolation via a transaction wrapper.

    Wraps each test in a top-level transaction that is rolled back at the end,
    so even committed inserts (from seeding) are undone between tests.

    :param db_engine: session-scoped engine fixture.
    :yields: an open :class:`sqlalchemy.orm.Session`.
    """

    engine = db_engine  # type: ignore[assignment]

    # Obtain a raw connection and begin a transaction on it.
    connection = engine.connect()  # type: ignore[attr-defined]
    trans = connection.begin()

    # Bind the session to this connection so all session actions go through it.
    session = Session(bind=connection)  # type: ignore[call-arg]

    # Patch session.commit() to emit a SAVEPOINT flush instead of a real commit,
    # so seed fixtures that call session.commit() don't escape the outer transaction.
    original_commit = session.commit

    def patched_commit() -> None:
        session.flush()

    session.commit = patched_commit  # type: ignore[method-assign]

    try:
        yield session
    finally:
        session.commit = original_commit  # type: ignore[method-assign]
        session.close()
        trans.rollback()
        connection.close()


@pytest.fixture()
def seeded_session(db_session: Session) -> Session:
    """Return a Session pre-loaded with the canonical SEED_PAPERS dataset.

    :param db_session: clean function-scoped session.
    :returns: the session with seed data inserted.
    """
    for paper in SEED_PAPERS:
        PaperRepository.add_paper(db_session, paper)
    db_session.commit()
    return db_session


@pytest.fixture()
def ephemeral_db_url(db_engine: object) -> str:
    """Return a SQLAlchemy DSN for the ephemeral test database.

    :param db_engine: session-scoped engine (ensures the DB is created).
    :returns: SQLAlchemy connection string for the test DB.
    """
    engine = db_engine  # type: ignore[attr-defined]
    return str(engine.url)  # type: ignore[union-attr]
