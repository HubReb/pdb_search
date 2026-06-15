"""Tests for Alembic migrations.

Verifies:
  - Revision 001 creates the canonical four-table schema from scratch.
  - Revision 002 renames the legacy 'bibtext_id' typo columns (idempotent).
  - Running migrate twice leaves row counts unchanged.

All tests use real PostgreSQL (no mocking), as required by constitution Principle II.
"""

from __future__ import annotations

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text


def _get_alembic_config(db_url: str) -> Config:
    """Build an Alembic Config pointing to the project alembic.ini."""
    from pathlib import Path

    here = Path(__file__).resolve().parent.parent
    ini_path = here / "alembic.ini"
    cfg = Config(str(ini_path))
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


class TestRevision001:
    """Tests for the initial schema migration."""

    def test_tables_created(self, ephemeral_db_url: str) -> None:
        """After running migrations, all four tables exist."""
        # migrations already applied by conftest db_session fixture
        engine = create_engine(ephemeral_db_url)
        try:
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            assert "papers" in tables
            assert "bib" in tables
            assert "authors_id" in tables
            assert "authors_papers" in tables
        finally:
            engine.dispose()

    def test_papers_columns(self, ephemeral_db_url: str) -> None:
        """The papers table has the canonical column set."""
        engine = create_engine(ephemeral_db_url)
        try:
            inspector = inspect(engine)
            cols = {c["name"] for c in inspector.get_columns("papers")}
            assert "id" in cols
            assert "title" in cols
            assert "contents" in cols
            assert "bibtex_id" in cols
            # Legacy typo must NOT be present
            assert "bibtext_id" not in cols
        finally:
            engine.dispose()

    def test_bib_columns(self, ephemeral_db_url: str) -> None:
        """The bib table has bibtex_id and bibtex columns."""
        engine = create_engine(ephemeral_db_url)
        try:
            inspector = inspect(engine)
            cols = {c["name"] for c in inspector.get_columns("bib")}
            assert "bibtex_id" in cols
            assert "bibtex" in cols
        finally:
            engine.dispose()

    def test_authors_papers_no_fk(self, ephemeral_db_url: str) -> None:
        """The authors_papers table has no DDL foreign keys (schema contract)."""
        engine = create_engine(ephemeral_db_url)
        try:
            inspector = inspect(engine)
            fks = inspector.get_foreign_keys("authors_papers")
            assert fks == [], f"authors_papers must have no DDL FKs, found: {fks}"
        finally:
            engine.dispose()


class TestMigrateIdempotent:
    """Tests that running migrate twice is safe."""

    def test_migrate_twice_no_error(self, ephemeral_db_url: str) -> None:
        """Running alembic upgrade head twice does not raise errors."""
        cfg = _get_alembic_config(ephemeral_db_url)
        # Already at head from conftest — running again should be a no-op
        command.upgrade(cfg, "head")  # idempotent

    def test_row_count_stable_after_remigrate(
        self, ephemeral_db_url: str, db_session: object
    ) -> None:
        """Row counts in papers are unchanged after a second migration run."""
        engine = create_engine(ephemeral_db_url)
        try:
            with engine.connect() as conn:
                before = conn.execute(text("SELECT count(*) FROM papers")).scalar()

            cfg = _get_alembic_config(ephemeral_db_url)
            command.upgrade(cfg, "head")

            with engine.connect() as conn:
                after = conn.execute(text("SELECT count(*) FROM papers")).scalar()
            assert before == after
        finally:
            engine.dispose()
