"""Integration tests for Alembic migrations.

Tests run against the ephemeral PostgreSQL DB.  Verifies:
- Revision 001 creates all four tables.
- Revision 002 handles the bibtext_id → bibtex_id rename idempotently.
"""

from __future__ import annotations

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text


def _make_alembic_cfg(url: str) -> Config:
    """Create an Alembic Config pointing at the given database URL.

    Args:
        url: SQLAlchemy-compatible connection string.

    Returns:
        Configured Alembic Config object.
    """
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", url)
    return cfg


def test_revision_001_creates_all_tables(ephemeral_db_url: str) -> None:
    """After running migrations, all four canonical tables exist."""
    engine = create_engine(ephemeral_db_url)
    insp = inspect(engine)
    table_names = set(insp.get_table_names())
    engine.dispose()
    assert "bib" in table_names
    assert "papers" in table_names
    assert "authors_id" in table_names
    assert "authors_papers" in table_names


def test_bib_table_has_unique_bibtex_column(ephemeral_db_url: str) -> None:
    """bib.bibtex column has a UNIQUE constraint (original DDL requirement)."""
    engine = create_engine(ephemeral_db_url)
    insp = inspect(engine)
    unique_constraints = insp.get_unique_constraints("bib")
    engine.dispose()
    constrained_cols = [col for uc in unique_constraints for col in uc["column_names"]]
    assert "bibtex" in constrained_cols, f"Expected UNIQUE on bibtex, got: {unique_constraints}"


def test_authors_papers_has_no_fk(ephemeral_db_url: str) -> None:
    """authors_papers has no DDL foreign key constraints (schema-preservation contract)."""
    engine = create_engine(ephemeral_db_url)
    insp = inspect(engine)
    fks = insp.get_foreign_keys("authors_papers")
    engine.dispose()
    assert fks == [], f"authors_papers should have no FK constraints but got: {fks}"


def test_revision_002_handles_bibtext_id_rename(postgresql_proc: object) -> None:
    """Revision 002 renames bibtext_id → bibtex_id if the typo column exists.

    Simulates a legacy database by:
    1. Running only revision 001 on a fresh DB.
    2. Manually renaming bibtex_id → bibtext_id to simulate the typo.
    3. Running revision 002 and verifying the column is back to bibtex_id.

    Args:
        postgresql_proc: The ephemeral PostgreSQL process fixture.
    """
    proc = postgresql_proc
    legacy_dbname = "paper_sorts_legacy_test"
    admin_url = (
        f"postgresql+psycopg://{proc.user}:@"  # type: ignore[union-attr]
        f"{proc.host}:{proc.port}/postgres"  # type: ignore[union-attr]
    )
    engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        conn.execute(text(f"DROP DATABASE IF EXISTS {legacy_dbname}"))
        conn.execute(text(f"CREATE DATABASE {legacy_dbname}"))
    engine.dispose()

    legacy_url = (
        f"postgresql+psycopg://{proc.user}:@"  # type: ignore[union-attr]
        f"{proc.host}:{proc.port}/{legacy_dbname}"  # type: ignore[union-attr]
    )
    # Run only revision 001
    cfg = _make_alembic_cfg(legacy_url)
    command.upgrade(cfg, "001")

    # Simulate typo: rename bibtex_id → bibtext_id in papers
    leg_engine = create_engine(legacy_url)
    with leg_engine.begin() as conn:
        conn.execute(text("ALTER TABLE papers RENAME COLUMN bibtex_id TO bibtext_id"))
    leg_engine.dispose()

    # Now run revision 002 — should rename it back
    command.upgrade(cfg, "002")

    leg_engine = create_engine(legacy_url)
    insp = inspect(leg_engine)
    col_names = [c["name"] for c in insp.get_columns("papers")]
    leg_engine.dispose()

    assert "bibtex_id" in col_names, f"Expected bibtex_id in papers, got: {col_names}"
    assert "bibtext_id" not in col_names


def test_revision_002_idempotent(ephemeral_db_url: str) -> None:
    """Running revision 002 twice on a canonical DB is safe (no-op second run)."""
    cfg = _make_alembic_cfg(ephemeral_db_url)
    # Already at head; running upgrade again is a no-op
    command.upgrade(cfg, "head")
    engine = create_engine(ephemeral_db_url)
    insp = inspect(engine)
    col_names = [c["name"] for c in insp.get_columns("papers")]
    engine.dispose()
    assert "bibtex_id" in col_names
    assert "bibtext_id" not in col_names
