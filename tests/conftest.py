"""Shared pytest fixtures: an ephemeral PostgreSQL and a seeded database.

``pytest-postgresql`` provisions a real PostgreSQL cluster from the host
``pg_ctl`` (constitution Principle II — persistence tests run against a real DB,
never a mock). No developer-local ``database.crypt``/``key`` is required.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from pytest_postgresql import factories
from pytest_postgresql.janitor import DatabaseJanitor
from sqlalchemy import Engine, text

from paper_sorts.db.session import create_db_engine, with_session
from tests.fixtures.seed_papers import SEED_PAPERS, SeedPaper

postgresql_proc = factories.postgresql_proc(executable="/usr/bin/pg_ctl")


def _run_migrations(database_url: str) -> None:
    """Apply all Alembic migrations to head against ``database_url``.

    :param database_url: the target database URL.
    """
    from alembic import command
    from alembic.config import Config

    cfg = Config("alembic.ini")
    cfg.attributes["sqlalchemy.url"] = database_url
    command.upgrade(cfg, "head")


@pytest.fixture(scope="session")
def ephemeral_db_url(postgresql_proc) -> Iterator[str]:  # type: ignore[no-untyped-def]
    """Yield a fresh ephemeral database URL with the schema migrated to head.

    :param postgresql_proc: the pytest-postgresql process fixture.
    :yields: a ``postgresql+psycopg://`` URL pointing at a migrated database.
    """
    user = postgresql_proc.user
    host = postgresql_proc.host
    port = postgresql_proc.port
    dbname = "paper_sorts_test"
    password = postgresql_proc.password or ""
    with DatabaseJanitor(
        user=user,
        host=host,
        port=port,
        dbname=dbname,
        version=postgresql_proc.version,
        password=password,
    ):
        credentials = f"{user}:{password}@" if password else f"{user}@"
        url = f"postgresql+psycopg://{credentials}{host}:{port}/{dbname}"
        _run_migrations(url)
        yield url


@pytest.fixture
def legacy_db_url(postgresql_proc) -> Iterator[str]:  # type: ignore[no-untyped-def]
    """Yield an isolated empty database URL for legacy-convergence testing.

    Separate from :func:`ephemeral_db_url` so migration-convergence tests can
    drop and recreate tables without disturbing the shared seeded session DB.

    :param postgresql_proc: the pytest-postgresql process fixture.
    :yields: a ``postgresql+psycopg://`` URL for a fresh empty database.
    """
    user = postgresql_proc.user
    host = postgresql_proc.host
    port = postgresql_proc.port
    dbname = "paper_sorts_legacy_test"
    password = postgresql_proc.password or ""
    with DatabaseJanitor(
        user=user,
        host=host,
        port=port,
        dbname=dbname,
        version=postgresql_proc.version,
        password=password,
    ):
        credentials = f"{user}:{password}@" if password else f"{user}@"
        yield f"postgresql+psycopg://{credentials}{host}:{port}/{dbname}"


@pytest.fixture
def engine(ephemeral_db_url: str) -> Iterator[Engine]:
    """Provide an engine bound to the ephemeral database, truncated per test.

    :param ephemeral_db_url: the migrated ephemeral database URL.
    :yields: a SQLAlchemy engine; all four tables are emptied before each test.
    """
    eng = create_db_engine(ephemeral_db_url)
    with eng.begin() as conn:
        conn.execute(
            text(
                "TRUNCATE authors_papers, authors_id, papers, bib "
                "RESTART IDENTITY CASCADE"
            )
        )
    yield eng
    eng.dispose()


def _insert_seed(engine: Engine, papers: list[SeedPaper]) -> None:
    """Insert the given seed papers (bib, paper, authors, links) into the DB.

    :param engine: the engine to write through.
    :param papers: the seed papers to insert.
    """
    from paper_sorts.db.models import AuthorId, AuthorPaper, Bib, Paper

    with with_session(engine) as session:
        for sp in papers:
            session.add(Bib(bibtex_id=sp.bibtex_id, bibtex=sp.bibtex))
            session.flush()
            paper = Paper(
                title=sp.title, contents=sp.contents, bibtex_id=sp.bibtex_id
            )
            session.add(paper)
            session.flush()
            for name in sp.authors:
                existing = (
                    session.query(AuthorId).filter(AuthorId.author == name).first()
                )
                if existing is None:
                    existing = AuthorId(author=name)
                    session.add(existing)
                    session.flush()
                session.add(
                    AuthorPaper(author_id=existing.id, paper_id=paper.id)
                )


@pytest.fixture
def seeded_engine(engine: Engine) -> Engine:
    """Provide an engine over a database loaded with ``SEED_PAPERS``.

    :param engine: the truncated ephemeral engine.
    :returns: the same engine, now seeded.
    """
    _insert_seed(engine, SEED_PAPERS)
    return engine
