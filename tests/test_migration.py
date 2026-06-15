"""Tests for the legacy-schema convergence migration (revision 002).

A database created with the misspelled ``bibtext_id`` column is migrated onto
the canonical ``bibtex_id`` spelling with zero data loss (row-count parity) and
idempotently (a rerun is a no-op).
"""

from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]

LEGACY_DDL = [
    "CREATE TABLE bib (bibtext_id text primary key, bibtext text);",
    "CREATE TABLE papers (id SERIAL PRIMARY KEY, title TEXT, contents TEXT, bibtext_id TEXT);",
    "CREATE TABLE authors_id (id SERIAL PRIMARY KEY, author TEXT);",
    "CREATE TABLE authors_papers (id SERIAL PRIMARY KEY, author_id INT, paper_id INT);",
]

LEGACY_ROWS = [
    "INSERT INTO bib VALUES ('k1', '@misc{k1}'), ('k2', '@misc{k2}');",
    "INSERT INTO papers (title, contents, bibtext_id) VALUES "
    "('T1', 'c1', 'k1'), ('T2', 'c2', 'k2');",
    "INSERT INTO authors_id (author) VALUES ('A, A'), ('B, B');",
    "INSERT INTO authors_papers (author_id, paper_id) VALUES (1, 1), (2, 2);",
]


def _alembic_config(url: str) -> Config:
    config = Config(str(PROJECT_ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(PROJECT_ROOT / "migrations"))
    config.set_main_option("sqlalchemy.url", url)
    return config


def _counts(url: str) -> dict[str, int]:
    engine = create_engine(url)
    with engine.connect() as conn:
        return {
            table: conn.execute(text(f"SELECT count(*) FROM {table}")).scalar_one()
            for table in ("bib", "papers", "authors_id", "authors_papers")
        }


def _stamp_to_revision_001(url: str) -> None:
    """Mark the hand-built legacy schema as if revision 001 already ran."""
    command.stamp(_alembic_config(url), "001")


def test_legacy_bibtext_id_converges_with_parity(ephemeral_db_url: str) -> None:
    """The migration renames the typo column and preserves all rows."""
    engine = create_engine(ephemeral_db_url)
    with engine.begin() as conn:
        for stmt in LEGACY_DDL + LEGACY_ROWS:
            conn.execute(text(stmt))
    before = _counts(ephemeral_db_url)

    _stamp_to_revision_001(ephemeral_db_url)
    command.upgrade(_alembic_config(ephemeral_db_url), "head")

    after = _counts(ephemeral_db_url)
    assert before == after  # zero data loss

    columns = {c["name"] for c in inspect(create_engine(ephemeral_db_url)).get_columns("papers")}
    assert "bibtex_id" in columns
    assert "bibtext_id" not in columns


def test_migration_is_idempotent(ephemeral_db_url: str) -> None:
    """Running the migration twice leaves a canonical database unchanged."""
    engine = create_engine(ephemeral_db_url)
    with engine.begin() as conn:
        for stmt in LEGACY_DDL + LEGACY_ROWS:
            conn.execute(text(stmt))
    _stamp_to_revision_001(ephemeral_db_url)
    command.upgrade(_alembic_config(ephemeral_db_url), "head")
    counts_once = _counts(ephemeral_db_url)
    # Re-run: already at head -> no-op.
    command.upgrade(_alembic_config(ephemeral_db_url), "head")
    assert _counts(ephemeral_db_url) == counts_once


def test_fresh_database_builds_canonical_schema(ephemeral_db_url: str) -> None:
    """On an empty database, migrating to head creates the canonical schema."""
    command.upgrade(_alembic_config(ephemeral_db_url), "head")
    inspector = inspect(create_engine(ephemeral_db_url))
    assert set(inspector.get_table_names()) >= {
        "bib",
        "papers",
        "authors_id",
        "authors_papers",
    }
    papers_cols = {c["name"] for c in inspector.get_columns("papers")}
    assert "bibtex_id" in papers_cols
