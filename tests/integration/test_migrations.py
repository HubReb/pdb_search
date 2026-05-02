"""Integration tests for the schema migration chain (T041).

Each test gets its own fresh, empty PG database via a function-scoped
``DatabaseJanitor`` so the four migration scenarios in
``contracts/database-schema.md`` § "Migration acceptance criteria"
each run on uncontaminated state:

1. Fresh DB -> revision 002 after upgrade head.
2. Modern DB (already at 001 with rows) -> upgrade head is a no-op.
3. Legacy DB (``bibtext_id`` columns) -> rename cleanly; row counts identical.
4. Mid-migration interrupt -> DB rolls back; re-run converges.

Tests run alembic directly rather than through the ``pdbsearch migrate``
subcommand so the assertions target migration semantics, not CLI plumbing.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
from alembic import command
from alembic import op as alembic_op
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from pytest_postgresql.executor import PostgreSQLExecutor
from pytest_postgresql.janitor import DatabaseJanitor
from sqlalchemy import create_engine, text


@pytest.fixture
def fresh_db_url(
    postgresql_proc: PostgreSQLExecutor, request: pytest.FixtureRequest
) -> Iterator[str]:
    """Yield a URL for a fresh empty database, scoped to this test."""
    test_id = re.sub(r"[^a-zA-Z0-9_]", "_", request.node.name).lower()[:40]
    db_name = f"mig_{test_id}"
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


def _alembic_cfg(url: str) -> Config:
    cfg = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", url)
    return cfg


def _row_counts(url: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    engine = create_engine(url)
    try:
        with engine.connect() as conn:
            for table in ("papers", "authors_id", "bib", "authors_papers"):
                try:
                    counts[table] = int(
                        conn.execute(text(f"SELECT count(*) FROM {table}")).scalar_one()  # noqa: S608
                    )
                except Exception:  # pre-migration tables may be absent
                    counts[table] = 0
    finally:
        engine.dispose()
    return counts


def _current_revision(url: str) -> str | None:
    engine = create_engine(url)
    try:
        with engine.connect() as conn:
            return MigrationContext.configure(conn).get_current_revision()
    finally:
        engine.dispose()


def _columns(url: str, table: str) -> set[str]:
    engine = create_engine(url)
    try:
        insp = sa.inspect(engine)
        return {c["name"] for c in insp.get_columns(table)}
    finally:
        engine.dispose()


def _create_legacy_schema(url: str) -> None:
    """Recreate the historical tables that ``paper_sorts/add.py`` produced."""
    engine = create_engine(url)
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE bib (bibtext_id text PRIMARY KEY, bibtex text UNIQUE)"))
            conn.execute(
                text(
                    "CREATE TABLE papers ("
                    "id SERIAL PRIMARY KEY, title text, contents text, "
                    "bibtext_id text REFERENCES bib(bibtext_id))"
                )
            )
            conn.execute(text("CREATE TABLE authors_id (id SERIAL PRIMARY KEY, author text)"))
            conn.execute(
                text(
                    "CREATE TABLE authors_papers ("
                    "id SERIAL PRIMARY KEY, author_id int, paper_id int)"
                )
            )
    finally:
        engine.dispose()


def _seed_legacy_rows(url: str) -> None:
    """Insert a small dataset into the legacy schema for round-trip count checks."""
    engine = create_engine(url)
    try:
        with engine.begin() as conn:
            conn.execute(
                text("INSERT INTO bib (bibtext_id, bibtex) VALUES (:k, :b)"),
                [{"k": "Legacy1", "b": "@article{Legacy1, year={2020}}"}],
            )
            conn.execute(
                text("INSERT INTO papers (title, contents, bibtext_id) VALUES (:t, :c, :b)"),
                [{"t": "Legacy Paper", "c": "summary", "b": "Legacy1"}],
            )
            conn.execute(
                text("INSERT INTO authors_id (author) VALUES (:a)"),
                [{"a": "Smith, A."}],
            )
            conn.execute(
                text(
                    "INSERT INTO authors_papers (author_id, paper_id) VALUES ("
                    "(SELECT id FROM authors_id WHERE author='Smith, A.'), "
                    "(SELECT id FROM papers WHERE bibtext_id='Legacy1'))"
                )
            )
    finally:
        engine.dispose()


def test_fresh_db_ends_at_002(fresh_db_url: str) -> None:
    command.upgrade(_alembic_cfg(fresh_db_url), "head")
    assert _current_revision(fresh_db_url) == "002"
    assert "bibtex_id" in _columns(fresh_db_url, "papers")


def test_modern_db_at_001_with_rows_is_noop_at_002(fresh_db_url: str) -> None:
    cfg = _alembic_cfg(fresh_db_url)
    command.upgrade(cfg, "001")

    engine = create_engine(fresh_db_url)
    try:
        with engine.begin() as conn:
            conn.execute(
                text("INSERT INTO bib (bibtex_id, bibtex) VALUES (:k, :b)"),
                [{"k": "Modern1", "b": "@article{Modern1, year={2025}}"}],
            )
            conn.execute(
                text("INSERT INTO papers (title, contents, bibtex_id) VALUES (:t, :c, :b)"),
                [{"t": "Modern Paper", "c": "summary", "b": "Modern1"}],
            )
    finally:
        engine.dispose()

    counts_before = _row_counts(fresh_db_url)
    command.upgrade(cfg, "head")
    counts_after = _row_counts(fresh_db_url)

    assert counts_before == counts_after
    assert _current_revision(fresh_db_url) == "002"
    papers_cols = _columns(fresh_db_url, "papers")
    assert "bibtex_id" in papers_cols
    assert "bibtext_id" not in papers_cols


def test_legacy_db_renames_in_place(fresh_db_url: str) -> None:
    _create_legacy_schema(fresh_db_url)
    _seed_legacy_rows(fresh_db_url)
    counts_before = _row_counts(fresh_db_url)

    command.upgrade(_alembic_cfg(fresh_db_url), "head")

    counts_after = _row_counts(fresh_db_url)
    assert counts_before == counts_after

    papers_cols = _columns(fresh_db_url, "papers")
    assert "bibtex_id" in papers_cols
    assert "bibtext_id" not in papers_cols

    bib_cols = _columns(fresh_db_url, "bib")
    assert "bibtex_id" in bib_cols
    assert "bibtext_id" not in bib_cols

    assert _current_revision(fresh_db_url) == "002"


def test_interrupted_legacy_migration_leaves_prior_state(
    fresh_db_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failure mid-002 rolls back via PG transactional DDL; re-run converges."""
    _create_legacy_schema(fresh_db_url)
    _seed_legacy_rows(fresh_db_url)

    original_alter = alembic_op.alter_column
    call_count = {"n": 0}

    def failing_alter(*args: Any, **kwargs: Any) -> Any:
        call_count["n"] += 1
        if call_count["n"] == 2:
            msg = "simulated mid-migration interrupt"
            raise RuntimeError(msg)
        return original_alter(*args, **kwargs)

    monkeypatch.setattr(alembic_op, "alter_column", failing_alter)

    cfg = _alembic_cfg(fresh_db_url)
    with pytest.raises(RuntimeError, match="simulated mid-migration interrupt"):
        command.upgrade(cfg, "head")

    # Pre-rename state must still be in place — neither alter_column committed.
    papers_cols = _columns(fresh_db_url, "papers")
    assert "bibtext_id" in papers_cols
    assert "bibtex_id" not in papers_cols

    # Re-run without the patch.
    monkeypatch.setattr(alembic_op, "alter_column", original_alter)
    command.upgrade(cfg, "head")

    assert "bibtex_id" in _columns(fresh_db_url, "papers")
    assert "bibtex_id" in _columns(fresh_db_url, "bib")
    assert _current_revision(fresh_db_url) == "002"
