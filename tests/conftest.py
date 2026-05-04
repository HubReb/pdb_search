"""Pytest configuration and ephemeral-database fixtures.

Per constitution Principle II (v1.3.0), persistence-layer tests run against
a real PostgreSQL instance provisioned ephemerally by ``pytest-postgresql``;
no mocking of the SQLAlchemy session, repositories, or driver is permitted.

The fixture chain (session-scoped unless noted):

* ``postgresql_proc`` — a single Postgres process spawned from the host's
  ``pg_ctl`` binary; reused across the whole test session.
* ``ephemeral_db_url`` — yields a SQLAlchemy URL pointing at a freshly
  created database; the database is dropped at session end.
* ``migrated_engine`` — runs ``alembic upgrade head`` against that URL so
  every test sees the canonical schema (revision ``head``).
* ``seeded_engine`` — inserts the canonical
  ``tests/fixtures/seed_papers.SEED_PAPERS`` dataset via the modern
  repository surface so every test starts from the same known rows.
* ``db_session`` (function-scoped) — a Session whose work is rolled back
  at teardown via the SQLAlchemy ``join_transaction_mode='create_savepoint'``
  pattern. Tests that call ``session.commit()`` create+release a SAVEPOINT
  inside an outer transaction that the fixture rolls back, so each test
  starts from the seeded state regardless of what the previous test did.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from pytest_postgresql import factories
from pytest_postgresql.executor import PostgreSQLExecutor
from pytest_postgresql.janitor import DatabaseJanitor
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from paper_sorts.db.repositories import PaperCreate, PaperRepository

# Spin up a single proc-level PostgreSQL using the host's pg_ctl binary.
# pytest-postgresql defaults to /usr/lib/postgresql/<n>/bin/pg_ctl and
# falls back to `pg_config --bindir`; neither resolves on this Fedora
# host, where pg_ctl lives at /usr/bin/pg_ctl, so pass it explicitly.
postgresql_proc = factories.postgresql_proc(
    port=None,  # let pytest-postgresql pick a free port
    unixsocketdir="/tmp",  # noqa: S108 — pytest-postgresql owns the per-run socket file lifecycle
    executable="/usr/bin/pg_ctl",
)


@pytest.fixture(scope="session")
def ephemeral_db_url(postgresql_proc: PostgreSQLExecutor) -> Iterator[str]:
    """Yield a SQLAlchemy URL pointing at a freshly-created database.

    The database is created when the fixture is first used and dropped
    at session end. The URL uses ``postgresql+psycopg://`` so SQLAlchemy
    binds the psycopg v3 driver per constitution Stack & Constraints.
    """
    db_name = "pdbsearch_test"
    with DatabaseJanitor(
        user=postgresql_proc.user,
        host=postgresql_proc.host,
        port=postgresql_proc.port,
        dbname=db_name,
        version=postgresql_proc.version,
        password=postgresql_proc.password,
    ):
        yield (
            f"postgresql+psycopg://{postgresql_proc.user}"
            f"@{postgresql_proc.host}:{postgresql_proc.port}/{db_name}"
        )


@pytest.fixture(scope="session")
def migrated_engine(ephemeral_db_url: str) -> Iterator[Engine]:
    """Run ``alembic upgrade head`` once per session and yield the bound engine.

    Uses the pre-attached-connection pattern that ``migrations/env.py``
    already supports: we ``begin()`` a connection, hand it to alembic via
    ``cfg.attributes['connection']``, and run ``upgrade(cfg, 'head')``
    inside that transaction. The ``with`` block commits on exit.
    """
    engine = create_engine(ephemeral_db_url, future=True)
    with engine.begin() as connection:
        cfg = Config(str(Path(__file__).resolve().parent.parent / "alembic.ini"))
        cfg.attributes["connection"] = connection
        command.upgrade(cfg, "head")
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def seeded_engine(migrated_engine: Engine) -> Engine:
    """Insert ``SEED_PAPERS`` once per session via the modern repository surface.

    Goes through ``PaperRepository.add`` rather than raw SQL so the seed
    exercises the same insert path the production code uses. Idempotent
    only at the boundary — running this fixture twice would raise on
    duplicate ``bibtex_id``, but session scope keeps it to a single run.
    """
    from tests.fixtures.seed_papers import SEED_PAPERS

    with Session(bind=migrated_engine) as session:
        repo = PaperRepository(session)
        for sp in SEED_PAPERS:
            repo.add(
                PaperCreate(
                    title=sp.title,
                    contents=sp.contents,
                    bibtex_id=sp.bibtex_id,
                    bibtex=sp.bibtex,
                    authors=tuple(sp.authors),
                )
            )
        session.commit()
    return migrated_engine


@pytest.fixture
def db_session(seeded_engine: Engine) -> Iterator[Session]:
    """Yield a Session whose work is rolled back at teardown.

    SQLAlchemy 2.x ``join_transaction_mode='create_savepoint'`` makes
    ``session.commit()`` release a SAVEPOINT inside the outer transaction
    rather than ending it; the fixture rolls back the outer transaction
    on exit, so every test starts from the seeded baseline regardless of
    what the previous test inserted, updated, or deleted.
    """
    connection = seeded_engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection, join_transaction_mode="create_savepoint")
    try:
        yield session
    finally:
        session.close()
        transaction.rollback()
        connection.close()


@pytest.fixture
def db_factory(seeded_engine: Engine) -> Iterator[sessionmaker[Session]]:
    """Yield a sessionmaker that CLI flows can call as ``ctx.obj``.

    Sibling of :func:`db_session` for tests that drive a Typer command
    (``cli.update.update(ctx)``, ``cli.delete.delete(ctx)``) rather than
    a service method directly. The factory creates sessions joining the
    fixture's outer transaction with ``join_transaction_mode='create_savepoint'``,
    so any number of inner ``with_session(factory)`` blocks the CLI opens
    commit cleanly to savepoints. The outer transaction rollback at
    teardown undoes every change the CLI made, restoring the seeded
    baseline for the next test.
    """
    connection = seeded_engine.connect()
    transaction = connection.begin()
    factory = sessionmaker(
        bind=connection,
        future=True,
        join_transaction_mode="create_savepoint",
    )
    try:
        yield factory
    finally:
        transaction.rollback()
        connection.close()
