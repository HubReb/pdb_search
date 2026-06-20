"""Real-DB tests for Alembic migrations (fresh, legacy-typo, idempotent)."""

from __future__ import annotations

from sqlalchemy import create_engine, inspect, text

from paper_sorts.cli.migrate import run_migrate


def _columns(url: str, table: str) -> set[str]:
    engine = create_engine(url, future=True)
    with engine.connect() as conn:
        cols = {c["name"] for c in inspect(conn).get_columns(table)}
    engine.dispose()
    return cols


def test_fresh_db_canonical_schema(ephemeral_db_url: str) -> None:
    run_migrate(ephemeral_db_url)
    assert _columns(ephemeral_db_url, "bib") == {"bibtex_id", "bibtex"}
    assert "bibtex_id" in _columns(ephemeral_db_url, "papers")


def test_migrate_idempotent(ephemeral_db_url: str) -> None:
    run_migrate(ephemeral_db_url)
    # rerun: must be a no-op, not an error
    run_migrate(ephemeral_db_url)
    assert _columns(ephemeral_db_url, "bib") == {"bibtex_id", "bibtex"}


def test_converges_legacy_typo_schema(ephemeral_db_url: str) -> None:
    # Build a legacy-typo database by hand (bibtext_id / bibtext columns),
    # then stamp it and converge it.
    engine = create_engine(ephemeral_db_url, future=True)
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE bib (bibtext_id text primary key, bibtext text);"
                "CREATE TABLE papers (id serial primary key, title text, contents text, bibtext_id text);"
                "CREATE TABLE authors_id (id serial primary key, author text);"
                "CREATE TABLE authors_papers (id serial primary key, author_id int, paper_id int);"
            )
        )
        conn.execute(text("INSERT INTO bib (bibtext_id, bibtext) VALUES ('K1', '@misc{K1}');"))
        conn.execute(
            text("INSERT INTO papers (title, contents, bibtext_id) VALUES ('T', 'C', 'K1');")
        )
        # stamp alembic at rev 001 so only the converger (002) runs
        conn.execute(text("CREATE TABLE alembic_version (version_num varchar(32) primary key);"))
        conn.execute(text("INSERT INTO alembic_version VALUES ('001_initial');"))
    engine.dispose()

    run_migrate(ephemeral_db_url)

    assert "bibtex_id" in _columns(ephemeral_db_url, "bib")
    assert "bibtext_id" not in _columns(ephemeral_db_url, "bib")
    engine = create_engine(ephemeral_db_url, future=True)
    with engine.connect() as conn:
        count = conn.execute(text("SELECT count(*) FROM papers")).scalar_one()
        bib_count = conn.execute(text("SELECT count(*) FROM bib")).scalar_one()
    engine.dispose()
    assert count == 1
    assert bib_count == 1
