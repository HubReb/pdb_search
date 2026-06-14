"""Integration tests for Alembic migration 001.

Tests:
- Fresh DB: revision 001 creates all four tables
- Legacy schema with bibtext_id (typo): revision 001 renames columns
- Already-migrated DB: revision 001 is idempotent
- Downgrade: tables are dropped

All tests run against an ephemeral PostgreSQL instance (no personal DB needed).
"""

from __future__ import annotations

import os
from pathlib import Path

import psycopg  # type: ignore[import]
from sqlalchemy import Engine, inspect, text

from paper_sorts.db.session import get_engine, with_session


def _make_fresh_engine(postgresql_proc: object, dbname: str) -> Engine:
    """Create a fresh ephemeral PostgreSQL database and return an engine for it.

    :param postgresql_proc: pytest-postgresql process fixture
    :param dbname: name for the new database
    :return: SQLAlchemy engine connected to the new database
    """
    proc = postgresql_proc  # type: ignore[assignment]
    host = getattr(proc, "host", "127.0.0.1")
    port = getattr(proc, "port", 5432)
    user = getattr(proc, "user", "postgres")

    dsn = f"host={host} port={port} user={user} dbname=postgres"
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f"DROP DATABASE IF EXISTS {dbname}")
        conn.execute(f"CREATE DATABASE {dbname}")

    db_url = f"postgresql+psycopg://{user}@{host}:{port}/{dbname}"
    return get_engine(db_url)


def _run_alembic_upgrade(engine: Engine, revision: str = "head") -> None:
    """Run Alembic upgrade on the given engine.

    :param engine: SQLAlchemy engine for the target database
    :param revision: Alembic revision to upgrade to
    """
    from alembic import command as alembic_cmd
    from alembic.config import Config

    # Set env var temporarily; restore original value when done
    old_url = os.environ.get("PDBSEARCH_DATABASE_URL")
    os.environ["PDBSEARCH_DATABASE_URL"] = str(engine.url)
    try:
        cli_dir = Path(__file__).parent.parent / "src" / "paper_sorts" / "cli"
        project_root = cli_dir.parent.parent.parent
        alembic_ini = project_root / "alembic.ini"
        assert alembic_ini.exists(), f"alembic.ini not found at {alembic_ini}"

        alembic_cfg = Config(str(alembic_ini))
        alembic_cmd.upgrade(alembic_cfg, revision)
    finally:
        if old_url is None:
            os.environ.pop("PDBSEARCH_DATABASE_URL", None)
        else:
            os.environ["PDBSEARCH_DATABASE_URL"] = old_url


def _run_alembic_downgrade(engine: Engine, revision: str = "base") -> None:
    """Run Alembic downgrade on the given engine.

    :param engine: SQLAlchemy engine for the target database
    :param revision: Alembic revision to downgrade to
    """
    from alembic import command as alembic_cmd
    from alembic.config import Config

    old_url = os.environ.get("PDBSEARCH_DATABASE_URL")
    os.environ["PDBSEARCH_DATABASE_URL"] = str(engine.url)
    try:
        cli_dir = Path(__file__).parent.parent / "src" / "paper_sorts" / "cli"
        project_root = cli_dir.parent.parent.parent
        alembic_ini = project_root / "alembic.ini"

        alembic_cfg = Config(str(alembic_ini))
        alembic_cmd.downgrade(alembic_cfg, revision)
    finally:
        if old_url is None:
            os.environ.pop("PDBSEARCH_DATABASE_URL", None)
        else:
            os.environ["PDBSEARCH_DATABASE_URL"] = old_url


class TestMigration001FreshDB:
    """Tests for migration 001 on a fresh empty database."""

    def test_creates_all_four_tables(self, postgresql_proc: object) -> None:
        """Revision 001 creates bib, papers, authors_id, authors_papers tables."""
        engine = _make_fresh_engine(postgresql_proc, "test_migration_fresh")
        _run_alembic_upgrade(engine)

        insp = inspect(engine)
        tables = insp.get_table_names()
        assert "bib" in tables
        assert "papers" in tables
        assert "authors_id" in tables
        assert "authors_papers" in tables

    def test_bib_table_has_canonical_columns(self, postgresql_proc: object) -> None:
        """Revision 001 creates bib table with bibtex_id (not bibtext_id)."""
        engine = _make_fresh_engine(postgresql_proc, "test_migration_bib_cols")
        _run_alembic_upgrade(engine)

        insp = inspect(engine)
        cols = {col["name"] for col in insp.get_columns("bib")}
        assert "bibtex_id" in cols
        assert "bibtex" in cols
        assert "bibtext_id" not in cols  # typo column must NOT appear

    def test_papers_table_has_canonical_columns(self, postgresql_proc: object) -> None:
        """Revision 001 creates papers table with bibtex_id FK column."""
        engine = _make_fresh_engine(postgresql_proc, "test_migration_papers_cols")
        _run_alembic_upgrade(engine)

        insp = inspect(engine)
        cols = {col["name"] for col in insp.get_columns("papers")}
        assert "id" in cols
        assert "title" in cols
        assert "contents" in cols
        assert "bibtex_id" in cols


class TestMigration001LegacySchema:
    """Tests for migration 001 handling legacy bibtext_id (typo) schema variants."""

    def _create_legacy_schema(self, engine: Engine) -> None:
        """Create the legacy schema with bibtext_id typo column in bib and papers."""
        with with_session(engine) as session:
            session.execute(text(
                "CREATE TABLE IF NOT EXISTS authors_papers "
                "(id SERIAL PRIMARY KEY, author_id INT, paper_id INT)"
            ))
            session.execute(text(
                "CREATE TABLE IF NOT EXISTS authors_id "
                "(id SERIAL PRIMARY KEY, author TEXT)"
            ))
            session.execute(text(
                "CREATE TABLE IF NOT EXISTS bib "
                "(bibtext_id TEXT PRIMARY KEY, bibtext TEXT)"  # typo columns
            ))
            session.execute(text(
                "CREATE TABLE IF NOT EXISTS papers "
                "(id SERIAL PRIMARY KEY, title TEXT, contents TEXT, bibtext_id TEXT, "
                "CONSTRAINT fk_bibtext_id FOREIGN KEY (bibtext_id) REFERENCES bib(bibtext_id))"
            ))

    def test_renames_bibtext_id_to_bibtex_id_in_bib(self, postgresql_proc: object) -> None:
        """Migration renames bibtext_id → bibtex_id in the bib table."""
        engine = _make_fresh_engine(postgresql_proc, "test_migration_legacy_bib")
        self._create_legacy_schema(engine)
        _run_alembic_upgrade(engine)

        insp = inspect(engine)
        cols = {col["name"] for col in insp.get_columns("bib")}
        assert "bibtex_id" in cols
        assert "bibtext_id" not in cols

    def test_renames_bibtext_id_to_bibtex_id_in_papers(self, postgresql_proc: object) -> None:
        """Migration renames bibtext_id → bibtex_id in the papers table."""
        engine = _make_fresh_engine(postgresql_proc, "test_migration_legacy_papers")
        self._create_legacy_schema(engine)
        _run_alembic_upgrade(engine)

        insp = inspect(engine)
        cols = {col["name"] for col in insp.get_columns("papers")}
        assert "bibtex_id" in cols
        assert "bibtext_id" not in cols

    def test_preserves_data_during_rename(self, postgresql_proc: object) -> None:
        """Migration preserves existing data when renaming legacy columns."""
        engine = _make_fresh_engine(postgresql_proc, "test_migration_data_preserve")
        self._create_legacy_schema(engine)

        # Insert data in legacy schema
        with with_session(engine) as session:
            session.execute(
                text("INSERT INTO bib (bibtext_id, bibtext) VALUES (:id, :bib)"),
                {"id": "Test2024", "bib": "@misc{Test2024}"},
            )
            session.execute(
                text(
                    "INSERT INTO papers (title, contents, bibtext_id) "
                    "VALUES (:title, :contents, :bibtext_id)"
                ),
                {"title": "Test Paper", "contents": "Summary.", "bibtext_id": "Test2024"},
            )

        _run_alembic_upgrade(engine)

        # Verify data preserved
        with with_session(engine) as session:
            bib_count = session.execute(text("SELECT COUNT(*) FROM bib")).scalar()
            papers_count = session.execute(text("SELECT COUNT(*) FROM papers")).scalar()
        assert bib_count == 1
        assert papers_count == 1


class TestMigration001Idempotent:
    """Tests for migration 001 idempotency — safe to run twice."""

    def test_upgrade_twice_is_safe(self, postgresql_proc: object) -> None:
        """Running upgrade head twice does not raise an error."""
        engine = _make_fresh_engine(postgresql_proc, "test_migration_idempotent")
        _run_alembic_upgrade(engine)
        # Second run should be a no-op, not raise
        _run_alembic_upgrade(engine)

        insp = inspect(engine)
        assert "bib" in insp.get_table_names()

    def test_upgrade_on_canonical_schema_is_noop(self, postgresql_proc: object) -> None:
        """Running upgrade on a DB already at canonical schema preserves all tables."""
        engine = _make_fresh_engine(postgresql_proc, "test_migration_canonical_noop")
        _run_alembic_upgrade(engine)
        _run_alembic_upgrade(engine)

        insp = inspect(engine)
        tables = insp.get_table_names()
        for table in ["bib", "papers", "authors_id", "authors_papers"]:
            assert table in tables


class TestMigration001Downgrade:
    """Tests for migration 001 downgrade."""

    def test_downgrade_removes_all_tables(self, postgresql_proc: object) -> None:
        """Downgrade to base drops all four tables."""
        engine = _make_fresh_engine(postgresql_proc, "test_migration_downgrade")
        _run_alembic_upgrade(engine)
        _run_alembic_downgrade(engine, "base")

        insp = inspect(engine)
        tables = insp.get_table_names()
        # Alembic tracking table may remain, but our tables should be gone
        for table in ["bib", "papers", "authors_id", "authors_papers"]:
            assert table not in tables
