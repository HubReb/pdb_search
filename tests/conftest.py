"""pytest fixtures for paper_sorts integration tests.

Session-scoped PostgreSQL is provisioned ephemerally by pytest-postgresql.
Alembic upgrade head runs once per test session.
Each test gets a fresh Session, with rollback on teardown.
"""

import pytest
from pytest_postgresql import factories
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from tests.fixtures.seed_papers import SEED_PAPERS

# Ephemeral PostgreSQL process — session-scoped so it starts once per test run.
postgresql_proc = factories.postgresql_proc(
    executable="/usr/bin/pg_ctl",
    port=None,  # auto-assign
)


@pytest.fixture(scope="session")
def ephemeral_db_url(postgresql_proc) -> str:  # type: ignore[no-untyped-def]
    """Return a postgresql+psycopg:// DSN for the ephemeral test database.

    Connects to the process as the superuser and creates a test database.

    :param postgresql_proc: pytest-postgresql process fixture.
    :returns: SQLAlchemy-compatible DSN string.
    """

    host = postgresql_proc.host
    port = postgresql_proc.port
    user = postgresql_proc.user

    # Connect to the default 'postgres' database and create test db
    admin_dsn = f"postgresql+psycopg://{user}@{host}:{port}/postgres"
    admin_engine = create_engine(
        admin_dsn,
        isolation_level="AUTOCOMMIT",
    )
    with admin_engine.connect() as conn:
        # Drop and recreate test database
        conn.execute(text("DROP DATABASE IF EXISTS papersorts_test"))
        conn.execute(text("CREATE DATABASE papersorts_test"))
    admin_engine.dispose()

    return f"postgresql+psycopg://{user}@{host}:{port}/papersorts_test"


@pytest.fixture(scope="session")
def db_engine(ephemeral_db_url: str):  # type: ignore[no-untyped-def]
    """Create a SQLAlchemy Engine pointing at the ephemeral DB.

    Runs Alembic upgrade head once per session to create the schema.

    :param ephemeral_db_url: DSN from ephemeral_db_url fixture.
    :returns: Configured SQLAlchemy Engine.
    """
    import os
    import pathlib

    from alembic import command
    from alembic.config import Config

    # Find alembic.ini at repo root
    repo_root = pathlib.Path(__file__).parent.parent
    alembic_ini = repo_root / "alembic.ini"

    os.environ["PDBSEARCH_DATABASE_URL"] = ephemeral_db_url
    alembic_cfg = Config(str(alembic_ini))
    alembic_cfg.set_main_option("sqlalchemy.url", ephemeral_db_url)
    command.upgrade(alembic_cfg, "head")

    engine = create_engine(ephemeral_db_url, echo=False)
    yield engine
    engine.dispose()


@pytest.fixture()
def db_session(db_engine):  # type: ignore[no-untyped-def]
    """Yield a SQLAlchemy Session for one test; roll back on teardown.

    :param db_engine: Session-scoped Engine fixture.
    :yields: Active :class:`sqlalchemy.orm.Session`.
    """
    connection = db_engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)
    try:
        yield session
    finally:
        session.close()
        transaction.rollback()
        connection.close()


@pytest.fixture()
def seeded_session(db_session: Session) -> Session:
    """Yield a Session pre-populated with SEED_PAPERS.

    Each test using this fixture gets a fresh seed; rollback on teardown
    (handled by db_session).

    :param db_session: Session fixture with rollback teardown.
    :returns: Session with seed data committed (within the rolled-back transaction).
    """
    from paper_sorts.db.repositories import PaperCreate, PaperRepository

    repo = PaperRepository(db_session)
    for paper_data in SEED_PAPERS:
        try:
            repo.add(PaperCreate(**paper_data))
        except ValueError:
            pass  # Skip duplicates if fixture is run multiple times within a session
    db_session.flush()
    return db_session
