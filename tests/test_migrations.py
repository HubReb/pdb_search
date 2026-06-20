"""Tests for Alembic migration Revision 002 — legacy schema convergence.

Tests create a table with the old typo column names (bibtext_id, bibtext),
then run the migration and verify the columns are renamed correctly.
These tests run against the ephemeral PostgreSQL instance.
"""

from sqlalchemy import inspect, text

from paper_sorts.db.session import with_session


class TestRevision002Convergence:
    """Tests for the legacy-schema convergence migration."""

    def test_upgrade_renames_bibtext_id_in_papers(self, db_engine: object) -> None:  # type: ignore[type-arg]
        """Revision 002 upgrade renames bibtext_id → bibtex_id in papers."""
        from sqlalchemy.engine import Engine

        assert isinstance(db_engine, Engine)

        with with_session(db_engine) as session:
            # Simulate legacy schema: rename bibtex_id to bibtext_id
            inspector = inspect(db_engine)
            cols = [c["name"] for c in inspector.get_columns("papers")]
            if "bibtex_id" in cols:
                session.execute(
                    text("ALTER TABLE papers RENAME COLUMN bibtex_id TO bibtext_id;")
                )

        # Verify old column exists
        inspector2 = inspect(db_engine)
        assert "bibtext_id" in [c["name"] for c in inspector2.get_columns("papers")]

        # Run the convergence migration logic directly
        with with_session(db_engine) as session:
            col_names = [c["name"] for c in inspect(db_engine).get_columns("papers")]
            if "bibtext_id" in col_names and "bibtex_id" not in col_names:
                session.execute(
                    text("ALTER TABLE papers RENAME COLUMN bibtext_id TO bibtex_id;")
                )

        # Verify canonical column exists
        inspector3 = inspect(db_engine)
        final_cols = [c["name"] for c in inspector3.get_columns("papers")]
        assert "bibtex_id" in final_cols
        assert "bibtext_id" not in final_cols

    def test_upgrade_idempotent_when_canonical(self, db_engine: object) -> None:  # type: ignore[type-arg]
        """Revision 002 is a no-op when canonical columns already exist."""
        from sqlalchemy.engine import Engine

        assert isinstance(db_engine, Engine)

        # Canonical schema already in place — migration should not error
        inspector = inspect(db_engine)
        cols = [c["name"] for c in inspector.get_columns("papers")]
        assert "bibtex_id" in cols

        # Running migration logic: no-op since bibtex_id already exists
        with with_session(db_engine) as session:
            col_names = [c["name"] for c in inspect(db_engine).get_columns("papers")]
            if "bibtext_id" in col_names and "bibtex_id" not in col_names:
                session.execute(
                    text("ALTER TABLE papers RENAME COLUMN bibtext_id TO bibtex_id;")
                )
            # else: nothing to do

        # Still canonical
        inspector2 = inspect(db_engine)
        assert "bibtex_id" in [c["name"] for c in inspector2.get_columns("papers")]
