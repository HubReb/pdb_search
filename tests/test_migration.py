"""Real-database tests for the migration command.

Verify that a fresh database upgrades to the four canonical tables, that a database carrying the
legacy ``bibtext_id``/``bibtext`` typo columns converges onto canonical names with row counts
preserved, and that the migration is idempotent on rerun.
"""

from __future__ import annotations

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text

from tests.conftest import _alembic_config


def _cfg(url: str) -> Config:
    """Build an Alembic config for the given URL.

    :param url: the database URL.
    :return: a configured Alembic config.
    """
    return _alembic_config(url)


def test_fresh_upgrade_creates_four_tables(ephemeral_db_url: str) -> None:
    """Upgrading a fresh database creates exactly the four canonical tables."""
    command.upgrade(_cfg(ephemeral_db_url), "head")
    engine = create_engine(ephemeral_db_url, future=True)
    try:
        tables = set(inspect(engine).get_table_names())
    finally:
        engine.dispose()
    assert {"papers", "bib", "authors_id", "authors_papers"} <= tables
    assert "bibtex_id" in {
        c["name"] for c in inspect(create_engine(ephemeral_db_url)).get_columns("papers")
    }


def test_rerun_is_idempotent(ephemeral_db_url: str) -> None:
    """Running the migration twice leaves the schema unchanged (no error)."""
    command.upgrade(_cfg(ephemeral_db_url), "head")
    command.upgrade(_cfg(ephemeral_db_url), "head")
    engine = create_engine(ephemeral_db_url, future=True)
    try:
        cols = {c["name"] for c in inspect(engine).get_columns("bib")}
    finally:
        engine.dispose()
    assert "bibtex_id" in cols
    assert "bibtext_id" not in cols


def test_converges_legacy_bibtext_columns(ephemeral_db_url: str) -> None:
    """A database with legacy ``bibtext_id``/``bibtext`` columns converges with row counts kept."""
    engine = create_engine(ephemeral_db_url, future=True)
    # Build the legacy-variant schema by hand, then stamp it at revision 001 so that 002 runs.
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE bib (bibtext_id text primary key, bibtext text)"))
        conn.execute(
            text(
                "CREATE TABLE papers (id SERIAL PRIMARY KEY, title TEXT, contents TEXT, "
                "bibtext_id TEXT)"
            )
        )
        conn.execute(text("CREATE TABLE authors_id (id SERIAL PRIMARY KEY, author TEXT)"))
        conn.execute(
            text("CREATE TABLE authors_papers (id SERIAL PRIMARY KEY, author_id INT, paper_id INT)")
        )
        conn.execute(text("INSERT INTO bib VALUES ('k1', '@article{k1}')"))
        conn.execute(
            text("INSERT INTO papers (title, contents, bibtext_id) VALUES ('t', 's', 'k1')")
        )
        conn.execute(text("INSERT INTO authors_id (author) VALUES ('Doe, Jane')"))
        conn.execute(text("INSERT INTO authors_papers (author_id, paper_id) VALUES (1, 1)"))

    command.stamp(_cfg(ephemeral_db_url), "001")

    with engine.connect() as conn:
        before = {
            "papers": conn.execute(text("SELECT count(*) FROM papers")).scalar_one(),
            "bib": conn.execute(text("SELECT count(*) FROM bib")).scalar_one(),
            "authors_id": conn.execute(text("SELECT count(*) FROM authors_id")).scalar_one(),
            "authors_papers": conn.execute(
                text("SELECT count(*) FROM authors_papers")
            ).scalar_one(),
        }

    command.upgrade(_cfg(ephemeral_db_url), "head")

    try:
        bib_cols = {c["name"] for c in inspect(engine).get_columns("bib")}
        paper_cols = {c["name"] for c in inspect(engine).get_columns("papers")}
        with engine.connect() as conn:
            after = {
                "papers": conn.execute(text("SELECT count(*) FROM papers")).scalar_one(),
                "bib": conn.execute(text("SELECT count(*) FROM bib")).scalar_one(),
                "authors_id": conn.execute(text("SELECT count(*) FROM authors_id")).scalar_one(),
                "authors_papers": conn.execute(
                    text("SELECT count(*) FROM authors_papers")
                ).scalar_one(),
            }
    finally:
        engine.dispose()

    assert "bibtex_id" in bib_cols and "bibtext_id" not in bib_cols
    assert "bibtex" in bib_cols and "bibtext" not in bib_cols
    assert "bibtex_id" in paper_cols and "bibtext_id" not in paper_cols
    assert before == after
