"""Shared pytest fixtures: an ephemeral PostgreSQL and a seeded engine.

A real PostgreSQL is provisioned per session by ``pytest-postgresql`` from the
host ``pg_ctl`` — no developer-local database or credentials are required.
Persistence tests run against this real database (no mocking of the session,
repositories, or driver).
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator

import pytest
from alembic import command
from alembic.config import Config
from pytest_postgresql import factories
from pytest_postgresql.janitor import DatabaseJanitor
from sqlalchemy import Engine, text

from paper_sorts.db.repositories import PaperRepository
from paper_sorts.db.session import create_db_engine, with_session
from tests.fixtures.seed_papers import SEED_PAPERS

postgresql_proc = factories.postgresql_proc(executable="/usr/bin/pg_ctl")


def _make_url(proc: object, dbname: str) -> str:
    """Build a SQLAlchemy URL for a database on the ephemeral server.

    :param proc: the ``postgresql_proc`` fixture value.
    :param dbname: name of the database to connect to.
    :return: a ``postgresql+psycopg://`` URL.
    """
    user = proc.user  # type: ignore[attr-defined]
    password = proc.password  # type: ignore[attr-defined]
    host = proc.host  # type: ignore[attr-defined]
    port = proc.port  # type: ignore[attr-defined]
    auth = f"{user}:{password}@" if password else f"{user}@"
    return f"postgresql+psycopg://{auth}{host}:{port}/{dbname}"


def _alembic_config(database_url: str) -> Config:
    """Build an Alembic config pointed at a specific database URL.

    :param database_url: the target database URL.
    :return: a configured :class:`alembic.config.Config`.
    """
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", database_url)
    return cfg


@pytest.fixture
def ephemeral_db_url(postgresql_proc: object) -> Iterator[str]:
    """Create a fresh empty database and yield its URL, dropping it after.

    :param postgresql_proc: the session-scoped ephemeral PostgreSQL process.
    :return: a URL to a fresh, empty database.
    """
    dbname = f"test_{uuid.uuid4().hex[:12]}"
    proc = postgresql_proc
    with DatabaseJanitor(
        user=proc.user,  # type: ignore[attr-defined]
        password=proc.password,  # type: ignore[attr-defined]
        host=proc.host,  # type: ignore[attr-defined]
        port=proc.port,  # type: ignore[attr-defined]
        dbname=dbname,
        version=proc.version,  # type: ignore[attr-defined]
    ):
        yield _make_url(proc, dbname)


@pytest.fixture
def migrated_engine(ephemeral_db_url: str) -> Iterator[Engine]:
    """Yield an engine for a database migrated to the canonical head schema.

    :param ephemeral_db_url: URL of a fresh empty database.
    :return: an engine bound to the migrated database.
    """
    command.upgrade(_alembic_config(ephemeral_db_url), "head")
    engine = create_db_engine(ephemeral_db_url)
    yield engine
    engine.dispose()


@pytest.fixture
def seeded_engine(migrated_engine: Engine) -> Engine:
    """Yield an engine whose database is migrated and loaded with SEED_PAPERS.

    :param migrated_engine: an engine on a migrated (empty) database.
    :return: the same engine, now seeded.
    """
    with with_session(migrated_engine) as session:
        repo = PaperRepository(session)
        for paper in SEED_PAPERS:
            repo.add(paper)
    return migrated_engine


@pytest.fixture
def seeded_db_url(ephemeral_db_url: str) -> str:
    """Migrate and seed a fresh database, returning its URL.

    Used by interface-layer tests that drive the CLI through ``--database-url``.

    :param ephemeral_db_url: URL of a fresh empty database.
    :return: the URL of a migrated, seeded database.
    """
    command.upgrade(_alembic_config(ephemeral_db_url), "head")
    engine = create_db_engine(ephemeral_db_url)
    try:
        with with_session(engine) as session:
            repo = PaperRepository(session)
            for paper in SEED_PAPERS:
                repo.add(paper)
    finally:
        engine.dispose()
    return ephemeral_db_url


@pytest.fixture
def legacy_engine(ephemeral_db_url: str) -> Iterator[Engine]:
    """Yield an engine on a database in the legacy ``bibtext_id`` (sic) schema.

    Builds the older procedural schema (typo column names) with a couple of rows
    so the migration command can be exercised end to end.

    :param ephemeral_db_url: URL of a fresh empty database.
    :return: an engine bound to the legacy-schema database.
    """
    engine = create_db_engine(ephemeral_db_url)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE bib (bibtext_id text primary key, bibtext text)"))
        conn.execute(
            text(
                "CREATE TABLE papers (id SERIAL PRIMARY KEY, title TEXT, "
                "contents TEXT, bibtext_id TEXT)"
            )
        )
        conn.execute(text("CREATE TABLE authors_id (id SERIAL PRIMARY KEY, author TEXT)"))
        conn.execute(
            text("CREATE TABLE authors_papers (id SERIAL PRIMARY KEY, author_id INT, paper_id INT)")
        )
        conn.execute(
            text("INSERT INTO bib (bibtext_id, bibtext) VALUES ('Legacy2019', '@x{Legacy2019}')")
        )
        conn.execute(
            text(
                "INSERT INTO papers (title, contents, bibtext_id) "
                "VALUES ('Legacy paper', 'summary', 'Legacy2019')"
            )
        )
        conn.execute(text("INSERT INTO authors_id (author) VALUES ('Old, Author')"))
        conn.execute(text("INSERT INTO authors_papers (author_id, paper_id) VALUES (1, 1)"))
    yield engine
    engine.dispose()
