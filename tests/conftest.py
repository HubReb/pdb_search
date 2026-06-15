"""Shared pytest fixtures: an ephemeral PostgreSQL and seeded sessions.

A real PostgreSQL server is provisioned per session by ``pytest-postgresql``
off the host ``pg_ctl`` — no developer-local database, no credentials. The
schema is built via Alembic so the migrations themselves are exercised. The
``seeded_session`` fixture loads the canonical dataset from
``tests/fixtures/seed_papers.SEED_PAPERS`` (Constitution Principle II).
"""

from __future__ import annotations

import shutil
from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from pytest_postgresql import factories
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from paper_sorts.db.models import AuthorId, AuthorPaper, Bib, Paper
from paper_sorts.db.session import create_db_engine, with_session
from tests.fixtures.seed_papers import SEED_PAPERS

_PG_CTL = shutil.which("pg_ctl") or "/usr/bin/pg_ctl"
postgresql_proc = factories.postgresql_proc(executable=_PG_CTL)
_postgresql = factories.postgresql("postgresql_proc")

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _make_url(connection_info: object) -> str:
    """Build a SQLAlchemy URL from a pytest-postgresql connection info object."""
    info = connection_info
    password = getattr(info, "password", "") or ""
    auth = info.user if not password else f"{info.user}:{password}"
    return f"postgresql+psycopg://{auth}@{info.host}:{info.port}/{info.dbname}"


def _alembic_config(database_url: str) -> Config:
    """Build an Alembic config for the test database."""
    config = Config(str(PROJECT_ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(PROJECT_ROOT / "migrations"))
    config.set_main_option("sqlalchemy.url", database_url)
    return config


@pytest.fixture
def ephemeral_db_url(_postgresql: object) -> str:
    """Return a SQLAlchemy URL for a fresh, empty ephemeral database."""
    return _make_url(_postgresql.info)


@pytest.fixture
def engine(ephemeral_db_url: str) -> Iterator[Engine]:
    """Yield an engine bound to an ephemeral DB migrated to head via Alembic."""
    command.upgrade(_alembic_config(ephemeral_db_url), "head")
    db_engine = create_db_engine(ephemeral_db_url)
    try:
        yield db_engine
    finally:
        db_engine.dispose()


@pytest.fixture
def seeded_session(engine: Engine) -> Iterator[Session]:
    """Yield a session whose database is pre-loaded with ``SEED_PAPERS``."""
    with with_session(engine) as session:
        _seed(session)
    with with_session(engine) as session:
        yield session


def _seed(session: Session) -> None:
    """Insert the canonical seed dataset into an empty schema."""
    author_ids: dict[str, int] = {}
    for paper in SEED_PAPERS:
        session.add(Bib(bibtex_id=paper.bibtex_id, bibtex=paper.bibtex))
        row = Paper(title=paper.title, contents=paper.summary, bibtex_id=paper.bibtex_id)
        session.add(row)
        session.flush()
        for name in paper.authors:
            if name not in author_ids:
                author = AuthorId(author=name)
                session.add(author)
                session.flush()
                author_ids[name] = author.id
            session.add(AuthorPaper(author_id=author_ids[name], paper_id=row.id))
