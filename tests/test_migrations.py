"""Migration tests: baseline creation and idempotent legacy convergence (US4)."""

from __future__ import annotations

from sqlalchemy import Engine, inspect, text


def _table_columns(engine: Engine, table: str) -> set[str]:
    """Return the live column names for ``table``.

    :param engine: engine to inspect.
    :param table: table name.
    :returns: the set of column names.
    """
    with engine.connect() as conn:
        return {c["name"] for c in inspect(conn).get_columns(table)}


def test_baseline_schema_created(engine: Engine) -> None:
    """Revision 0001 creates the four canonical tables with canonical columns."""
    with engine.connect() as conn:
        tables = set(inspect(conn).get_table_names())
    assert {"bib", "papers", "authors_id", "authors_papers"} <= tables
    assert "bibtex_id" in _table_columns(engine, "bib")
    assert "bibtex" in _table_columns(engine, "bib")
    assert "bibtex_id" in _table_columns(engine, "papers")


def test_authors_papers_has_no_foreign_keys(engine: Engine) -> None:
    """The link table preserves its no-FK design (schema-preservation contract)."""
    with engine.connect() as conn:
        fks = inspect(conn).get_foreign_keys("authors_papers")
    assert fks == []


def _build_legacy_schema(engine: Engine) -> None:
    """Recreate a legacy ``bibtext_id`` (sic) database with two rows.

    :param engine: engine over a fresh database to overwrite with legacy DDL.
    """
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS authors_papers, authors_id, papers, bib CASCADE"))
        conn.execute(text("CREATE TABLE bib (bibtext_id text primary key, bibtext text)"))
        conn.execute(
            text(
                "CREATE TABLE papers (id SERIAL PRIMARY KEY, title TEXT, contents TEXT, "
                "bibtext_id TEXT)"
            )
        )
        conn.execute(text("CREATE TABLE authors_id (id SERIAL PRIMARY KEY, author TEXT)"))
        conn.execute(
            text(
                "CREATE TABLE authors_papers (id SERIAL PRIMARY KEY, author_id INT, paper_id INT)"
            )
        )
        conn.execute(text("INSERT INTO bib VALUES ('k1', 'b1'), ('k2', 'b2')"))
        conn.execute(
            text(
                "INSERT INTO papers (title, contents, bibtext_id) VALUES "
                "('t1', 'c1', 'k1'), ('t2', 'c2', 'k2')"
            )
        )
        conn.execute(text("INSERT INTO authors_id (author) VALUES ('A, X'), ('B, Y')"))
        conn.execute(
            text("INSERT INTO authors_papers (author_id, paper_id) VALUES (1, 1), (2, 2)")
        )


def _counts(engine: Engine) -> dict[str, int]:
    """Return row counts for the four tables.

    :param engine: engine to count through.
    :returns: mapping of table name to row count.
    """
    with engine.connect() as conn:
        return {
            t: conn.execute(text(f"SELECT count(*) FROM {t}")).scalar_one()
            for t in ("bib", "papers", "authors_id", "authors_papers")
        }


def _run_converge(database_url: str) -> None:
    """Apply revision 0002's convergence logic against ``database_url``.

    :param database_url: the database URL whose legacy schema to converge.
    """
    from alembic import command
    from alembic.config import Config

    cfg = Config("alembic.ini")
    cfg.attributes["sqlalchemy.url"] = database_url
    # Stamp as baseline (tables already exist), then upgrade to head runs 0002.
    command.stamp(cfg, "0001")
    command.upgrade(cfg, "head")


def test_legacy_convergence_preserves_rows_and_is_idempotent(
    legacy_db_url: str,
) -> None:
    """Converging a legacy ``bibtext_id`` DB renames columns with zero row loss.

    Rerunning the convergence is a no-op (US4 AS3 idempotency). Runs against its
    own isolated database so it does not disturb the shared seeded session DB.
    """
    from paper_sorts.db.session import create_db_engine

    eng = create_db_engine(legacy_db_url)
    _build_legacy_schema(eng)
    before = _counts(eng)

    _run_converge(legacy_db_url)

    assert "bibtex_id" in _table_columns(eng, "bib")
    assert "bibtex" in _table_columns(eng, "bib")
    assert "bibtex_id" in _table_columns(eng, "papers")
    assert "bibtext_id" not in _table_columns(eng, "bib")
    assert _counts(eng) == before

    # Idempotent rerun: convergence again changes nothing.
    _run_converge(legacy_db_url)
    assert _counts(eng) == before
    assert "bibtex_id" in _table_columns(eng, "bib")
    eng.dispose()
