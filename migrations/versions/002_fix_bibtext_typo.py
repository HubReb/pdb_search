"""Fix legacy bibtext_id typo column names.

Some databases were created by the legacy ``add.py`` / ``get_data.py`` modules
which used the column name ``bibtext_id`` (sic) instead of the canonical
``bibtex_id``. This migration detects and renames those columns so that
both historical schema variants converge to the canonical schema.

The migration is idempotent: it checks whether the typo column exists before
attempting to rename it, so running it against an already-canonical database
is safe.

Revision ID: 002
Revises: 001
Create Date: 2026-04-26
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.engine import Connection

# Revision identifiers used by Alembic.
revision: str = "002"
down_revision: str | None = "001"
branch_labels: str | None = None
depends_on: str | None = None


def _column_exists(conn: Connection, table: str, column: str) -> bool:
    """Return True if *column* exists in *table* on the current connection.

    Args:
        conn: Active SQLAlchemy connection.
        table: Table name to inspect.
        column: Column name to check.

    Returns:
        True if the column exists, False otherwise.
    """
    result = conn.execute(
        sa.text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = :tbl AND column_name = :col"
        ),
        {"tbl": table, "col": column},
    )
    return result.fetchone() is not None


def upgrade() -> None:
    """Rename bibtext_id → bibtex_id in bib and papers tables if the typo exists.

    Checks each table independently, so a partially-migrated database (where
    one table was already renamed manually) is handled gracefully.
    """
    conn = op.get_bind()

    # Fix bib table: bibtext_id → bibtex_id
    if _column_exists(conn, "bib", "bibtext_id"):
        op.alter_column("bib", "bibtext_id", new_column_name="bibtex_id")

    # Fix papers table: bibtext_id → bibtex_id
    if _column_exists(conn, "papers", "bibtext_id"):
        op.alter_column("papers", "bibtext_id", new_column_name="bibtex_id")


def downgrade() -> None:
    """Reverse the rename: bibtex_id → bibtext_id (restores the typo variant).

    Only renames if the canonical column exists (i.e. upgrade() was applied).
    """
    conn = op.get_bind()

    if _column_exists(conn, "bib", "bibtex_id"):
        op.alter_column("bib", "bibtex_id", new_column_name="bibtext_id")

    if _column_exists(conn, "papers", "bibtex_id"):
        op.alter_column("papers", "bibtex_id", new_column_name="bibtext_id")
