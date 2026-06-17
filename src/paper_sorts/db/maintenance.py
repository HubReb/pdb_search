"""Schema-maintenance helpers for the one-shot migration command.

All database inspection and the legacy-column convergence live here so that the
CLI ``migrate`` command never imports ``sqlalchemy`` directly (Principle I:
persistence-layer isolation). The Alembic plumbing (``upgrade``/``stamp``) stays
in the migration command, which is admin/scripted tooling; this module owns the
raw schema inspection and the in-place legacy rename.
"""

from __future__ import annotations

from sqlalchemy import inspect, text

from paper_sorts.db.session import create_db_engine

#: Canonical tables whose row counts the migration command preserves.
COUNTED_TABLES = ("papers", "authors_id", "authors_papers", "bib")


def list_tables(database_url: str) -> set[str]:
    """Return the set of table names present in the database.

    :param database_url: the target database URL.
    :return: the existing table names.
    """
    engine = create_db_engine(database_url)
    try:
        return set(inspect(engine).get_table_names())
    finally:
        engine.dispose()


def row_counts(database_url: str) -> dict[str, int]:
    """Return per-table row counts for the canonical tables that exist.

    :param database_url: the target database URL.
    :return: mapping of table name to row count (absent tables omitted).
    """
    engine = create_db_engine(database_url)
    counts: dict[str, int] = {}
    try:
        existing = set(inspect(engine).get_table_names())
        with engine.connect() as conn:
            for table in COUNTED_TABLES:
                if table in existing:
                    counts[table] = conn.execute(
                        text(f"SELECT count(*) FROM {table}")  # noqa: S608 - fixed names
                    ).scalar_one()
    finally:
        engine.dispose()
    return counts


def converge_legacy_columns(database_url: str) -> None:
    """Rename legacy ``bibtext_id`` (sic) columns to canonical ``bibtex_id``.

    Idempotent and in-place: each rename is guarded on the current column set, so
    a fresh or already-canonical database is a no-op. Renaming preserves rows.

    :param database_url: the target database URL.
    """
    engine = create_db_engine(database_url)
    try:
        with engine.begin() as conn:
            cols = {
                (row[0], row[1])
                for row in conn.execute(
                    text(
                        "SELECT table_name, column_name FROM information_schema.columns "
                        "WHERE table_name IN ('papers', 'bib')"
                    )
                )
            }
            for table in ("papers", "bib"):
                if (table, "bibtext_id") in cols and (table, "bibtex_id") not in cols:
                    conn.execute(text(f"ALTER TABLE {table} RENAME COLUMN bibtext_id TO bibtex_id"))
            if ("bib", "bibtext") in cols and ("bib", "bibtex") not in cols:
                conn.execute(text("ALTER TABLE bib RENAME COLUMN bibtext TO bibtex"))
    finally:
        engine.dispose()
