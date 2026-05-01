"""Pytest configuration and ephemeral-database fixtures.

Per constitution Principle II (v1.3.0), persistence-layer tests run against
a real PostgreSQL instance provisioned ephemerally by ``pytest-postgresql``;
no mocking of the SQLAlchemy session, repositories, or driver is permitted.

This module provides a session-scoped fixture exposing a ready-to-use
PostgreSQL connection URL. Per the plan, schema management (Alembic
``upgrade head``) is wired in once Alembic itself lands in T013/T014;
until then, tests that need a schema must create their own tables or
skip with ``pytest.mark.skip(reason="schema not yet under Alembic")``.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from pytest_postgresql import factories
from pytest_postgresql.executor import PostgreSQLExecutor
from pytest_postgresql.janitor import DatabaseJanitor

# Spin up a single proc-level PostgreSQL using the host's pg_ctl binary.
# pytest-postgresql defaults to /usr/lib/postgresql/<n>/bin/pg_ctl and
# falls back to `pg_config --bindir`; neither resolves on this Fedora
# host, where pg_ctl lives at /usr/bin/pg_ctl, so pass it explicitly.
postgresql_proc = factories.postgresql_proc(
    port=None,  # let pytest-postgresql pick a free port
    unixsocketdir="/tmp",
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
