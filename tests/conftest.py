"""Shared pytest fixtures: an ephemeral PostgreSQL with the migrated schema.

The database is provisioned per session by pytest-postgresql off the host's
``pg_ctl`` — no developer-local database, ``database.crypt``, or ``key`` file is
required (constitution Principle II; spec US3).
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from pytest_postgresql import factories
from pytest_postgresql.janitor import DatabaseJanitor
from sqlalchemy import Engine

sys.path.insert(0, str(Path(__file__).parent / "fixtures"))

from seed_papers import load_seed  # noqa: E402

from paper_sorts.cli.migrate import run_migrate  # noqa: E402
from paper_sorts.db.session import make_engine  # noqa: E402

postgresql_proc = factories.postgresql_proc(executable="/usr/bin/pg_ctl")


@pytest.fixture
def ephemeral_db_url(postgresql_proc) -> Iterator[str]:  # type: ignore[no-untyped-def]
    """Create a fresh database on the ephemeral cluster and yield its URL."""
    proc = postgresql_proc
    dbname = "pdbsearch_test"
    with DatabaseJanitor(
        user=proc.user,
        host=proc.host,
        port=proc.port,
        dbname=dbname,
        version=proc.version,
        password=proc.password,
    ):
        password = f":{proc.password}" if proc.password else ""
        yield f"postgresql+psycopg://{proc.user}{password}@{proc.host}:{proc.port}/{dbname}"


@pytest.fixture
def migrated_engine(ephemeral_db_url: str) -> Engine:
    """Yield an engine against a database migrated to head (canonical schema)."""
    run_migrate(ephemeral_db_url)
    return make_engine(ephemeral_db_url)


@pytest.fixture
def seeded_engine(migrated_engine: Engine) -> Engine:
    """Yield a migrated engine loaded with :data:`SEED_PAPERS`."""
    load_seed(migrated_engine)
    return migrated_engine
