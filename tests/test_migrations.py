"""Integration tests for Alembic migrations.

Tests that migration 001 creates all four tables and that migration 002
handles both historical schema variants (bibtex_id and bibtext_id typo).

Each migration test creates its own isolated database within the shared
ephemeral proc, named with a unique suffix, so tests do not interfere
with the ORM tables used by repository tests.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest
from pytest_postgresql.janitor import DatabaseJanitor
from sqlalchemy import create_engine, inspect, text


def _make_alembic_cfg(url: str) -> object:
    """Build an AlembicConfig pointing at the repo's alembic.ini.

    :param url: SQLAlchemy database URL for the test DB.
    :returns: configured :class:`alembic.config.Config`.
    """
    from alembic.config import Config as AlembicConfig

    ini_path = Path(__file__).parent.parent / "alembic.ini"
    cfg = AlembicConfig(str(ini_path))
    cfg.set_main_option("sqlalchemy.url", url)
    return cfg


@pytest.fixture()
def migration_db(postgresql_proc: object) -> object:  # type: ignore[type-arg]
    """Provide a URL for an isolated migration test database.

    Creates a unique database within the shared proc, yields the URL and an
    inspection engine, then drops the database after the test.

    :param postgresql_proc: the running PostgreSQL proc fixture.
    :yields: tuple of (url: str, engine: Engine).
    """

    proc = postgresql_proc  # type: ignore[attr-defined]
    user: str = proc.user
    host: str = proc.host
    port: int = proc.port
    version = proc.version
    template_dbname = getattr(proc, "template_dbname", None)

    # Unique name per test to avoid collisions.
    dbname = f"migration_test_{uuid.uuid4().hex[:8]}"
    url = f"postgresql+psycopg://{user}@{host}:{port}/{dbname}"

    with DatabaseJanitor(
        user=user, host=host, port=port, dbname=dbname,
        version=version, template_dbname=template_dbname,
    ):
        engine = create_engine(url)
        yield url, engine
        engine.dispose()


def test_migration_001_creates_tables(migration_db: tuple) -> None:
    """Migration 001 creates all four expected tables."""
    url, engine = migration_db
    cfg = _make_alembic_cfg(url)

    from alembic import command as alembic_command
    alembic_command.upgrade(cfg, "001")

    insp = inspect(engine)
    table_names = insp.get_table_names()
    assert "papers" in table_names
    assert "bib" in table_names
    assert "authors_id" in table_names
    assert "authors_papers" in table_names


def test_migration_001_idempotent(migration_db: tuple) -> None:
    """Migration 001 is safe to run twice (IF NOT EXISTS on all tables)."""
    url, engine = migration_db

    from alembic import command as alembic_command
    cfg = _make_alembic_cfg(url)
    alembic_command.upgrade(cfg, "001")
    alembic_command.downgrade(cfg, "base")
    alembic_command.upgrade(cfg, "001")

    insp = inspect(engine)
    assert "papers" in insp.get_table_names()


def test_migration_002_canonical_schema_noop(migration_db: tuple) -> None:
    """Migration 002 is a no-op when columns already use the canonical names."""
    url, engine = migration_db

    from alembic import command as alembic_command
    cfg = _make_alembic_cfg(url)
    alembic_command.upgrade(cfg, "002")

    insp = inspect(engine)
    bib_cols = {c["name"] for c in insp.get_columns("bib")}
    paper_cols = {c["name"] for c in insp.get_columns("papers")}

    assert "bibtex_id" in bib_cols
    assert "bibtext_id" not in bib_cols
    assert "bibtex_id" in paper_cols
    assert "bibtext_id" not in paper_cols


def test_migration_002_renames_legacy_columns(migration_db: tuple) -> None:
    """Migration 002 renames bibtext_id (typo) to bibtex_id in bib and papers."""
    url, engine = migration_db

    # Manually create the legacy schema (with typo columns).
    with engine.connect() as conn:
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS bib (bibtext_id TEXT PRIMARY KEY, bibtext TEXT)"
        ))
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS papers "
            "(id SERIAL PRIMARY KEY, title TEXT, contents TEXT, bibtext_id TEXT)"
        ))
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS authors_id (id SERIAL PRIMARY KEY, author TEXT)"
        ))
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS authors_papers "
            "(id SERIAL PRIMARY KEY, author_id INT, paper_id INT)"
        ))
        # Mark migration 001 as applied.
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS alembic_version (version_num VARCHAR(32) NOT NULL)"
        ))
        conn.execute(text("INSERT INTO alembic_version VALUES ('001')"))
        conn.commit()

    from alembic import command as alembic_command
    cfg = _make_alembic_cfg(url)
    alembic_command.upgrade(cfg, "002")

    insp = inspect(engine)
    bib_cols = {c["name"] for c in insp.get_columns("bib")}
    paper_cols = {c["name"] for c in insp.get_columns("papers")}

    assert "bibtex_id" in bib_cols
    assert "bibtext_id" not in bib_cols
    assert "bibtex_id" in paper_cols
    assert "bibtext_id" not in paper_cols


def test_full_migration_chain(migration_db: tuple) -> None:
    """Running both migrations in sequence produces a valid schema."""
    url, engine = migration_db

    from alembic import command as alembic_command
    cfg = _make_alembic_cfg(url)
    alembic_command.upgrade(cfg, "head")

    insp = inspect(engine)
    table_names = set(insp.get_table_names())
    assert {"papers", "bib", "authors_id", "authors_papers"}.issubset(table_names)

    paper_cols = {c["name"] for c in insp.get_columns("papers")}
    assert "bibtex_id" in paper_cols
    assert "bibtext_id" not in paper_cols
