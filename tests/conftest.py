"""Shared pytest fixtures: an ephemeral PostgreSQL and seeded sessions.

The suite spins up an ephemeral PostgreSQL via ``pytest-postgresql`` off the
host ``pg_ctl`` (constitution Principle II, FR-008) — no developer-local
database, no ``database.crypt``/``key``. Each test gets a fresh database; the
canonical schema is built from the ORM metadata and seeded from
``tests/fixtures/seed_papers.SEED_PAPERS``.
"""

from __future__ import annotations

import shutil
from collections.abc import Iterator

import pytest
from pytest_postgresql import factories
from pytest_postgresql.executor import PostgreSQLExecutor
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from paper_sorts.db.models import Base
from paper_sorts.db.repositories import PaperRepository
from paper_sorts.db.session import make_engine, with_session
from tests.fixtures.seed_papers import SEED_PAPERS

_PG_CTL = shutil.which("pg_ctl") or "/usr/bin/pg_ctl"
_PG_BIN = _PG_CTL.rsplit("/", 1)[0]

postgresql_proc = factories.postgresql_proc(executable=f"{_PG_BIN}/pg_ctl")
postgresql = factories.postgresql("postgresql_proc")


def _url_from_executor(proc: PostgreSQLExecutor, dbname: str) -> str:
    """Build a SQLAlchemy URL from a running pytest-postgresql executor.

    :param proc: the running PostgreSQL process fixture.
    :param dbname: the database name to connect to.
    :returns: a ``postgresql+psycopg://`` URL.
    """
    user = proc.user
    password = proc.password
    auth = f"{user}:{password}@" if password else f"{user}@"
    return f"postgresql+psycopg://{auth}{proc.host}:{proc.port}/{dbname}"


@pytest.fixture
def ephemeral_db_url(postgresql) -> str:  # noqa: ANN001
    """Return a SQLAlchemy URL for the per-test ephemeral database.

    :param postgresql: the pytest-postgresql connection fixture.
    :returns: the connection URL.
    """
    info = postgresql.info
    user = info.user
    password = info.password or ""
    auth = f"{user}:{password}@" if password else f"{user}@"
    return f"postgresql+psycopg://{auth}{info.host}:{info.port}/{info.dbname}"


@pytest.fixture
def engine(ephemeral_db_url: str) -> Iterator[Engine]:
    """Provide an engine bound to a freshly schema-created ephemeral database.

    :param ephemeral_db_url: the per-test database URL.
    :yields: a ready-to-use engine.
    """
    eng = make_engine(ephemeral_db_url)
    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def seeded_engine(engine: Engine) -> Engine:
    """Provide an engine whose database is seeded with ``SEED_PAPERS``.

    :param engine: the schema-created engine.
    :returns: the same engine, now populated.
    """
    with with_session(engine) as session:
        repo = PaperRepository(session)
        for paper in SEED_PAPERS:
            repo.add(paper)
    return engine


@pytest.fixture
def session(engine: Engine) -> Iterator[Session]:
    """Provide an open session on the empty schema (caller manages commits).

    :param engine: the schema-created engine.
    :yields: an open session, closed on teardown.
    """
    with with_session(engine) as sess:
        yield sess
