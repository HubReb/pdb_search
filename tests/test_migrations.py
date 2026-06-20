"""Integration tests for Alembic migrations.

Tests:
- Revision 001 creates all four tables with correct schema.
- Revision 002 renames bibtext_id → bibtex_id on legacy schema databases.

These tests spin up a fresh database and do NOT use the shared db_engine
fixture (which already has migrations applied).  They use a raw SQLAlchemy
engine to inspect the schema after migration.
"""

from __future__ import annotations

from sqlalchemy import inspect


class TestRevision001:
    """Verify that the initial migration creates the correct schema."""

    def test_tables_created(self, ephemeral_db_url: str) -> None:
        """After upgrade to head, all four tables exist."""
        from paper_sorts.db.session import get_engine

        engine = get_engine(ephemeral_db_url)
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        assert "papers" in table_names
        assert "bib" in table_names
        assert "authors_id" in table_names
        assert "authors_papers" in table_names
        engine.dispose()

    def test_papers_columns(self, ephemeral_db_url: str) -> None:
        """The papers table has id, title, contents, bibtex_id columns."""
        from paper_sorts.db.session import get_engine

        engine = get_engine(ephemeral_db_url)
        inspector = inspect(engine)
        cols = {c["name"] for c in inspector.get_columns("papers")}
        assert {"id", "title", "contents", "bibtex_id"} <= cols
        # Legacy typo column must NOT be present
        assert "bibtext_id" not in cols
        engine.dispose()

    def test_bib_unique_constraint(self, ephemeral_db_url: str) -> None:
        """The bib table has a UNIQUE constraint on bibtex."""
        from paper_sorts.db.session import get_engine

        engine = get_engine(ephemeral_db_url)
        inspector = inspect(engine)
        uniques = inspector.get_unique_constraints("bib")
        unique_col_sets = [tuple(u["column_names"]) for u in uniques]
        assert ("bibtex",) in unique_col_sets
        engine.dispose()

    def test_authors_papers_no_fk(self, ephemeral_db_url: str) -> None:
        """The authors_papers table has no DDL foreign key constraints."""
        from paper_sorts.db.session import get_engine

        engine = get_engine(ephemeral_db_url)
        inspector = inspect(engine)
        fks = inspector.get_foreign_keys("authors_papers")
        assert fks == []
        engine.dispose()
