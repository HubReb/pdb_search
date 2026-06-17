"""Migration-command tests against a real ephemeral PostgreSQL.

Covers the three convergence cases: an untracked legacy ``bibtext_id`` (sic)
schema with rows, an already-canonical seeded schema (no-op), and a fresh empty
database. Row counts (papers, authors, authorships, bib) must be preserved, and
re-running the command must be idempotent.
"""

from __future__ import annotations

from sqlalchemy import inspect, text

from paper_sorts.cli.migrate import run_migrate
from paper_sorts.db.session import create_db_engine


def _build_legacy_schema(url: str) -> None:
    """Create the legacy ``bibtext_id`` (sic) schema with a couple of rows.

    :param url: the target database URL.
    """
    engine = create_db_engine(url)
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE bib (bibtext_id text primary key, bibtext text)"))
            conn.execute(
                text(
                    "CREATE TABLE papers (id SERIAL PRIMARY KEY, title TEXT, "
                    "contents TEXT, bibtext_id TEXT)"
                )
            )
            conn.execute(text("CREATE TABLE authors_id (id SERIAL PRIMARY KEY, author TEXT)"))
            conn.execute(
                text(
                    "CREATE TABLE authors_papers (id SERIAL PRIMARY KEY, "
                    "author_id INT, paper_id INT)"
                )
            )
            conn.execute(
                text("INSERT INTO bib (bibtext_id, bibtext) VALUES ('L2019', '@x{L2019}')")
            )
            conn.execute(
                text(
                    "INSERT INTO papers (title, contents, bibtext_id) "
                    "VALUES ('Legacy paper', 's', 'L2019')"
                )
            )
            conn.execute(text("INSERT INTO authors_id (author) VALUES ('Old, Author')"))
            conn.execute(text("INSERT INTO authors_papers (author_id, paper_id) VALUES (1, 1)"))
    finally:
        engine.dispose()


def _counts(url: str) -> dict[str, int]:
    """Return canonical-table row counts for the database at ``url``.

    :param url: the target database URL.
    :return: mapping of table name to row count.
    """
    engine = create_db_engine(url)
    try:
        with engine.connect() as conn:
            return {
                t: conn.execute(text(f"SELECT count(*) FROM {t}")).scalar_one()
                for t in ("papers", "authors_id", "authors_papers", "bib")
            }
    finally:
        engine.dispose()


def _columns(url: str, table: str) -> set[str]:
    """Return the column names of a table.

    :param url: the target database URL.
    :param table: the table to inspect.
    :return: the set of column names.
    """
    engine = create_db_engine(url)
    try:
        return {col["name"] for col in inspect(engine).get_columns(table)}
    finally:
        engine.dispose()


def test_migrate_legacy_schema_preserves_rows(ephemeral_db_url: str) -> None:
    """A legacy bibtext_id schema converges to canonical with identical counts."""
    _build_legacy_schema(ephemeral_db_url)
    before = _counts(ephemeral_db_url)

    run_migrate(ephemeral_db_url)

    after = _counts(ephemeral_db_url)
    assert after == before
    assert "bibtex_id" in _columns(ephemeral_db_url, "papers")
    assert "bibtext_id" not in _columns(ephemeral_db_url, "papers")
    assert "bibtex" in _columns(ephemeral_db_url, "bib")


def test_migrate_legacy_is_idempotent(ephemeral_db_url: str) -> None:
    """Re-running migrate on a converged legacy DB is a clean no-op."""
    _build_legacy_schema(ephemeral_db_url)
    run_migrate(ephemeral_db_url)
    first = _counts(ephemeral_db_url)
    run_migrate(ephemeral_db_url)
    assert _counts(ephemeral_db_url) == first


def test_migrate_canonical_seeded_is_noop(seeded_db_url: str) -> None:
    """Migrating an already-canonical seeded DB preserves all rows."""
    before = _counts(seeded_db_url)
    run_migrate(seeded_db_url)
    assert _counts(seeded_db_url) == before


def test_migrate_fresh_database_creates_schema(ephemeral_db_url: str) -> None:
    """Migrating an empty database creates the canonical schema at head."""
    run_migrate(ephemeral_db_url)
    engine = create_db_engine(ephemeral_db_url)
    try:
        tables = set(inspect(engine).get_table_names())
    finally:
        engine.dispose()
    assert {"papers", "bib", "authors_id", "authors_papers"} <= tables
    assert "bibtex_id" in _columns(ephemeral_db_url, "papers")
