"""Migration tests: legacy-typo convergence with zero data loss, idempotent."""

from __future__ import annotations

from sqlalchemy import Engine, text

from paper_sorts.cli.migrate import run_migrate
from paper_sorts.db.session import with_session


def _build_legacy_typo_schema(engine: Engine) -> None:
    """Create a database in the legacy ``bibtext_id`` (typo) schema and seed it."""
    with with_session(engine) as session:
        session.execute(text("CREATE TABLE bib (bibtext_id text primary key, bibtext text)"))
        session.execute(
            text(
                "CREATE TABLE papers (id SERIAL PRIMARY KEY, title text, "
                "contents text, bibtext_id text)"
            )
        )
        session.execute(text("CREATE TABLE authors_id (id SERIAL PRIMARY KEY, author text)"))
        session.execute(
            text("CREATE TABLE authors_papers (id SERIAL PRIMARY KEY, author_id int, paper_id int)")
        )
        session.execute(text("INSERT INTO bib VALUES ('K1', '@a{K1}'), ('K2', '@b{K2}')"))
        session.execute(
            text(
                "INSERT INTO papers (title, contents, bibtext_id) VALUES "
                "('P1', 's1', 'K1'), ('P2', 's2', 'K2')"
            )
        )
        session.execute(text("INSERT INTO authors_id (author) VALUES ('A, X'), ('B, Y')"))
        session.execute(
            text("INSERT INTO authors_papers (author_id, paper_id) VALUES (1, 1), (2, 2)")
        )


def _counts(engine: Engine) -> dict[str, int]:
    """Return row counts for the four tables."""
    with with_session(engine) as session:
        return {
            t: session.execute(text(f"SELECT count(*) FROM {t}")).scalar_one()
            for t in ("papers", "bib", "authors_id", "authors_papers")
        }


def _columns(engine: Engine, table: str) -> set[str]:
    """Return the column names of a table."""
    with with_session(engine) as session:
        rows = session.execute(
            text("SELECT column_name FROM information_schema.columns WHERE table_name = :t"),
            {"t": table},
        ).scalars()
        return set(rows)


def test_migrate_converges_legacy_typo_schema(ephemeral_db_url: str) -> None:
    from paper_sorts.db.session import make_engine

    engine = make_engine(ephemeral_db_url)
    _build_legacy_typo_schema(engine)
    before = _counts(engine)

    # Stamp the legacy DB at revision 001 so the converge revision (002) runs.
    from alembic import command

    from paper_sorts.cli.migrate import _alembic_config

    command.stamp(_alembic_config(ephemeral_db_url), "001_initial")
    run_migrate(ephemeral_db_url)

    assert "bibtex_id" in _columns(engine, "papers")
    assert "bibtext_id" not in _columns(engine, "papers")
    assert "bibtex_id" in _columns(engine, "bib")
    assert "bibtex" in _columns(engine, "bib")
    assert _counts(engine) == before  # zero data loss

    # Content spot-check.
    with with_session(engine) as session:
        title = session.execute(
            text("SELECT title FROM papers WHERE bibtex_id = 'K1'")
        ).scalar_one()
    assert title == "P1"
    engine.dispose()


def test_migrate_is_idempotent(ephemeral_db_url: str) -> None:
    from alembic import command

    from paper_sorts.cli.migrate import _alembic_config
    from paper_sorts.db.session import make_engine

    engine = make_engine(ephemeral_db_url)
    _build_legacy_typo_schema(engine)
    command.stamp(_alembic_config(ephemeral_db_url), "001_initial")
    run_migrate(ephemeral_db_url)
    before = _counts(engine)
    run_migrate(ephemeral_db_url)  # second run: no-op
    assert _counts(engine) == before
    assert "bibtex_id" in _columns(engine, "papers")
    engine.dispose()


def test_migrate_on_fresh_canonical_db(ephemeral_db_url: str) -> None:
    # A fresh empty DB upgrades to the canonical schema via 001, then 002 is a
    # no-op (no legacy typo columns present).
    from paper_sorts.db.session import make_engine

    run_migrate(ephemeral_db_url)
    engine = make_engine(ephemeral_db_url)
    assert "bibtex_id" in _columns(engine, "papers")
    assert "bibtex" in _columns(engine, "bib")
    engine.dispose()
