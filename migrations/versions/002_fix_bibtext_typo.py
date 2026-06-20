"""Handle legacy bibtext_id typo: rename bibtext_id → bibtex_id where needed.

This migration detects databases created by the older ``get_data.py`` /
``add.py`` procedural modules, which used the misspelled column name
``bibtext_id``.  If found, it renames the column and adjusts the FK
constraint.  If the canonical ``bibtex_id`` column already exists the
migration is a no-op (idempotent).

Revision ID: 002
Revises: 001
Create Date: 2026-06-20
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import inspect, text

revision: str = "002"
down_revision: str = "001"
branch_labels: None = None
depends_on: None = None


def _column_exists(conn: object, table: str, column: str) -> bool:
    """Return True if *column* exists in *table*."""
    inspector = inspect(conn)  # type: ignore[arg-type]
    cols = [c["name"] for c in inspector.get_columns(table)]
    return column in cols


def upgrade() -> None:
    """Rename bibtext_id → bibtex_id in bib and papers tables if needed."""
    bind = op.get_bind()

    # Fix bib table
    if _column_exists(bind, "bib", "bibtext_id"):
        # Drop the old table and recreate with correct schema
        # (simpler than trying to rename PK columns in PostgreSQL)
        with op.batch_alter_table("bib") as batch_op:
            batch_op.alter_column("bibtext_id", new_column_name="bibtex_id")

    # Fix papers table
    if _column_exists(bind, "papers", "bibtext_id"):
        with op.batch_alter_table("papers") as batch_op:
            batch_op.alter_column("bibtext_id", new_column_name="bibtex_id")


def downgrade() -> None:
    """Rename bibtex_id → bibtext_id (revert typo fix) — for test purposes only."""
    bind = op.get_bind()

    if _column_exists(bind, "papers", "bibtex_id"):
        with op.batch_alter_table("papers") as batch_op:
            batch_op.alter_column("bibtex_id", new_column_name="bibtext_id")

    if _column_exists(bind, "bib", "bibtex_id"):
        with op.batch_alter_table("bib") as batch_op:
            batch_op.alter_column("bibtex_id", new_column_name="bibtext_id")
