"""Integration tests for Alembic migrations.

Tests: upgrade head from empty DB, downgrade to base, re-upgrade (idempotent),
and revision 002's legacy bibtext_id column rename.
"""

import os
import pathlib

import pytest
from pytest_postgresql import factories
from sqlalchemy import create_engine, inspect, text

# Dedicated process for migration tests (separate from the main test DB)
migration_postgresql_proc = factories.postgresql_proc(
    executable="/usr/bin/pg_ctl",
    port=None,
)


@pytest.fixture(scope="module")
def migration_db_url(migration_postgresql_proc) -> str:  # type: ignore[no-untyped-def]
    """DSN for the migration-test-only ephemeral database (module-scoped)."""
    from sqlalchemy import create_engine as _ce

    host = migration_postgresql_proc.host
    port = migration_postgresql_proc.port
    user = migration_postgresql_proc.user

    admin_dsn = f"postgresql+psycopg://{user}@{host}:{port}/postgres"
    admin_engine = _ce(admin_dsn, isolation_level="AUTOCOMMIT")
    with admin_engine.connect() as conn:
        conn.execute(text("DROP DATABASE IF EXISTS migration_test"))
        conn.execute(text("CREATE DATABASE migration_test"))
    admin_engine.dispose()

    return f"postgresql+psycopg://{user}@{host}:{port}/migration_test"


@pytest.fixture(scope="module")
def alembic_cfg_path() -> str:
    """Path to alembic.ini at repo root."""
    repo_root = pathlib.Path(__file__).parent.parent
    return str(repo_root / "alembic.ini")


class TestMigrations:
    """Tests for Alembic migration correctness and idempotency."""

    def test_upgrade_head_creates_tables(
        self, migration_db_url: str, alembic_cfg_path: str
    ) -> None:
        """upgrade head creates all four expected tables."""
        from alembic import command
        from alembic.config import Config

        os.environ["PDBSEARCH_DATABASE_URL"] = migration_db_url
        cfg = Config(alembic_cfg_path)
        cfg.set_main_option("sqlalchemy.url", migration_db_url)
        command.upgrade(cfg, "head")

        engine = create_engine(migration_db_url)
        with engine.connect() as conn:
            inspector = inspect(conn)
            tables = inspector.get_table_names()
        engine.dispose()

        assert "bib" in tables
        assert "papers" in tables
        assert "authors_id" in tables
        assert "authors_papers" in tables

    def test_downgrade_to_base_drops_tables(
        self, migration_db_url: str, alembic_cfg_path: str
    ) -> None:
        """downgrade base removes all four tables."""
        from alembic import command
        from alembic.config import Config

        os.environ["PDBSEARCH_DATABASE_URL"] = migration_db_url
        cfg = Config(alembic_cfg_path)
        cfg.set_main_option("sqlalchemy.url", migration_db_url)
        command.downgrade(cfg, "base")

        engine = create_engine(migration_db_url)
        with engine.connect() as conn:
            inspector = inspect(conn)
            tables = inspector.get_table_names()
        engine.dispose()

        # After downgrade to base, data tables should be gone
        assert "bib" not in tables
        assert "papers" not in tables

    def test_upgrade_twice_idempotent(
        self, migration_db_url: str, alembic_cfg_path: str
    ) -> None:
        """Running upgrade head twice on the same DB does not raise."""
        from alembic import command
        from alembic.config import Config

        os.environ["PDBSEARCH_DATABASE_URL"] = migration_db_url
        cfg = Config(alembic_cfg_path)
        cfg.set_main_option("sqlalchemy.url", migration_db_url)
        command.upgrade(cfg, "head")
        # Run again — should be a no-op
        command.upgrade(cfg, "head")


class TestLegacyColumnMigration:
    """Tests for revision 002 handling of bibtext_id → bibtex_id rename."""

    @pytest.fixture(scope="class")
    def legacy_db_url(self, migration_postgresql_proc) -> str:  # type: ignore[no-untyped-def]
        """DSN for a fresh DB where we manually create the legacy schema."""
        from sqlalchemy import create_engine as _ce

        host = migration_postgresql_proc.host
        port = migration_postgresql_proc.port
        user = migration_postgresql_proc.user

        admin_dsn = f"postgresql+psycopg://{user}@{host}:{port}/postgres"
        admin_engine = _ce(admin_dsn, isolation_level="AUTOCOMMIT")
        with admin_engine.connect() as conn:
            conn.execute(text("DROP DATABASE IF EXISTS legacy_test"))
            conn.execute(text("CREATE DATABASE legacy_test"))
        admin_engine.dispose()

        return f"postgresql+psycopg://{user}@{host}:{port}/legacy_test"

    def test_revision_002_renames_bibtext_id(
        self, legacy_db_url: str, alembic_cfg_path: str
    ) -> None:
        """Revision 002 renames bibtext_id (sic) to bibtex_id if present."""
        from alembic import command
        from alembic.config import Config

        os.environ["PDBSEARCH_DATABASE_URL"] = legacy_db_url
        cfg = Config(alembic_cfg_path)
        cfg.set_main_option("sqlalchemy.url", legacy_db_url)
        # Upgrade to revision 001 only (creates bibtex_id column)
        command.upgrade(cfg, "001")

        # Manually rename to simulate legacy schema with typo
        engine = create_engine(legacy_db_url)
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE papers RENAME COLUMN bibtex_id TO bibtext_id"))
            conn.commit()
        engine.dispose()

        # Now upgrade to head — revision 002 should rename it back
        command.upgrade(cfg, "head")

        engine = create_engine(legacy_db_url)
        with engine.connect() as conn:
            inspector = inspect(conn)
            cols = [c["name"] for c in inspector.get_columns("papers")]
        engine.dispose()

        assert "bibtex_id" in cols
        assert "bibtext_id" not in cols

    def test_revision_002_idempotent_on_correct_schema(
        self, legacy_db_url: str, alembic_cfg_path: str
    ) -> None:
        """Revision 002 is a no-op when bibtex_id already exists (idempotent)."""
        from alembic import command
        from alembic.config import Config

        os.environ["PDBSEARCH_DATABASE_URL"] = legacy_db_url
        cfg = Config(alembic_cfg_path)
        cfg.set_main_option("sqlalchemy.url", legacy_db_url)
        # Already at head from previous test — downgrade then upgrade again
        command.downgrade(cfg, "base")
        command.upgrade(cfg, "head")

        engine = create_engine(legacy_db_url)
        with engine.connect() as conn:
            inspector = inspect(conn)
            cols = [c["name"] for c in inspector.get_columns("papers")]
        engine.dispose()

        assert "bibtex_id" in cols
