"""Test configuration and shared fixtures for paper_sorts.

The ephemeral PostgreSQL instance is created per test session via
pytest-postgresql, using the host's pg_ctl binary at /usr/bin/pg_ctl.
No personal database, credentials file, or external state required.

Session-scope: one PG process, one engine, per pytest run.
Function-scope: each test gets a clean session that rolls back after the test.
"""

import pytest
from pytest_postgresql import factories
from pytest_postgresql.janitor import DatabaseJanitor
from sqlalchemy.orm import Session

from paper_sorts.db.models import Base
from paper_sorts.db.session import get_engine

# ---------------------------------------------------------------------------
# Ephemeral PostgreSQL process fixture (session-scoped via factory)
# ---------------------------------------------------------------------------
# Uses the host pg_ctl binary; no Docker required.
postgresql_proc = factories.postgresql_proc(
    executable="/usr/bin/pg_ctl",
    port=None,  # auto-assign a free port
)


# ---------------------------------------------------------------------------
# Session-scoped engine
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def ephemeral_db_url(postgresql_proc: object) -> str:  # type: ignore[type-arg]
    """Return a SQLAlchemy URL for the ephemeral PostgreSQL instance.

    Uses DatabaseJanitor to create the test database in the ephemeral proc.

    :param postgresql_proc: pytest-postgresql process fixture.
    :returns: SQLAlchemy-compatible ``postgresql+psycopg://`` URL.
    """
    proc = postgresql_proc
    host: str = proc.host  # type: ignore[attr-defined]
    port: int = proc.port  # type: ignore[attr-defined]
    user: str = proc.user  # type: ignore[attr-defined]
    dbname: str = proc.dbname  # type: ignore[attr-defined]
    version: object = proc.version  # type: ignore[attr-defined]
    template: str = proc.template_dbname  # type: ignore[attr-defined]

    # Create the test database via DatabaseJanitor
    with DatabaseJanitor(
        user=user,
        host=host,
        port=port,
        version=version,  # type: ignore[arg-type]
        dbname=dbname,
        template_dbname=template,
    ):
        yield f"postgresql+psycopg://{user}@{host}:{port}/{dbname}"


@pytest.fixture(scope="session")
def db_engine(ephemeral_db_url: str) -> object:  # type: ignore[type-arg]
    """Create a SQLAlchemy engine and all ORM tables for the test session.

    :param ephemeral_db_url: URL produced by :func:`ephemeral_db_url`.
    :returns: Configured :class:`~sqlalchemy.engine.Engine`.
    """
    from sqlalchemy.engine import Engine

    engine: Engine = get_engine(ephemeral_db_url)
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)
    engine.dispose()


# ---------------------------------------------------------------------------
# Function-scoped session (rolls back after each test)
# ---------------------------------------------------------------------------


@pytest.fixture
def db_session(db_engine: object) -> object:  # type: ignore[type-arg]
    """Yield a database session that rolls back after each test.

    This ensures tests are isolated without recreating tables.

    :param db_engine: Session-scoped engine fixture.
    :yields: Open :class:`~sqlalchemy.orm.Session`.
    """
    from sqlalchemy.engine import Engine

    assert isinstance(db_engine, Engine)
    session = Session(db_engine)
    try:
        yield session
    finally:
        session.rollback()
        session.close()
