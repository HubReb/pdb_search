"""Shared pytest fixtures.

An ephemeral PostgreSQL is provisioned per test session by pytest-postgresql off the host
``pg_ctl`` — no developer-local database, ``database.crypt``, or ``key`` file is required. The
schema is created by running the Alembic migrations against the ephemeral database, so the
tests exercise the same DDL path the application ships. Per the constitution, persistence tests
run against this real database; the session/repositories/driver are never mocked.
"""

from __future__ import annotations

import shutil
from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from pytest_postgresql import factories
from pytest_postgresql.janitor import DatabaseJanitor
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from paper_sorts.db.repositories import (
    AuthorRepository,
    BibRepository,
    PaperRepository,
)
from paper_sorts.db.session import create_db_engine, with_session
from tests.fixtures.seed_papers import SEED_PAPERS

_PG_CTL = shutil.which("pg_ctl") or "/usr/bin/pg_ctl"

postgresql_proc = factories.postgresql_proc(executable=_PG_CTL)


@pytest.fixture
def ephemeral_db_url(postgresql_proc: object) -> Iterator[str]:
    """Provision a fresh database on the ephemeral cluster and yield its URL.

    :param postgresql_proc: the pytest-postgresql process fixture.
    :return: a ``postgresql+psycopg://`` URL for a freshly created database.
    """
    proc = postgresql_proc
    dbname = "pdbsearch_test"
    janitor = DatabaseJanitor(
        user=proc.user,  # type: ignore[attr-defined]
        host=proc.host,  # type: ignore[attr-defined]
        port=proc.port,  # type: ignore[attr-defined]
        dbname=dbname,
        version=proc.version,  # type: ignore[attr-defined]
        password=proc.password,  # type: ignore[attr-defined]
    )
    janitor.init()
    user = proc.user  # type: ignore[attr-defined]
    password = proc.password  # type: ignore[attr-defined]
    host = proc.host  # type: ignore[attr-defined]
    port = proc.port  # type: ignore[attr-defined]
    auth = f"{user}:{password}" if password else user
    url = f"postgresql+psycopg://{auth}@{host}:{port}/{dbname}"
    try:
        yield url
    finally:
        janitor.drop()


def _alembic_config(url: str) -> Config:
    """Build an Alembic config pointed at the repo's migrations and the given URL.

    :param url: the database URL to migrate.
    :return: a configured :class:`alembic.config.Config`.
    """
    repo_root = Path(__file__).resolve().parent.parent
    cfg = Config(str(repo_root / "alembic.ini"))
    cfg.set_main_option("script_location", str(repo_root / "migrations"))
    cfg.set_main_option("sqlalchemy.url", url)
    return cfg


@pytest.fixture
def engine(ephemeral_db_url: str) -> Iterator[Engine]:
    """Yield an engine bound to a freshly migrated ephemeral database.

    :param ephemeral_db_url: the URL of the ephemeral database.
    :return: a SQLAlchemy engine after ``alembic upgrade head``.
    """
    command.upgrade(_alembic_config(ephemeral_db_url), "head")
    eng = create_db_engine(ephemeral_db_url)
    try:
        yield eng
    finally:
        eng.dispose()


def _seed(session: Session) -> None:
    """Load ``SEED_PAPERS`` into the database via the repositories.

    :param session: an open session bound to the migrated database.
    """
    papers = PaperRepository(session)
    bibs = BibRepository(session)
    authors = AuthorRepository(session)
    for paper in SEED_PAPERS:
        bibs.add(paper.bibtex_id, paper.bibtex)
        paper_id = papers.add_paper_row(paper.title, paper.summary, paper.bibtex_id)
        for author in paper.authors:
            author_id = authors.get_or_create_author_id(author)
            authors.link(author_id, paper_id)


@pytest.fixture
def seeded_engine(engine: Engine) -> Engine:
    """Return an engine whose database has been loaded with ``SEED_PAPERS``.

    :param engine: the migrated ephemeral engine.
    :return: the same engine, now seeded.
    """
    with with_session(engine) as session:
        _seed(session)
    return engine
