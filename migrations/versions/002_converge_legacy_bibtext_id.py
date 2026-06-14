"""Converge legacy bibtext_id typo variant.

Revision ID: 002
Revises: 001
Create Date: 2026-06-15

Some historical databases have the ``bibtext_id`` (sic — typo) column in both
``papers`` and ``bib`` tables instead of ``bibtex_id``.  This migration
detects the typo variant and renames the column to the canonical spelling.

Idempotent: skips the rename if ``bibtex_id`` already exists.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import inspect, text

revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_column(conn: object, table: str, column: str) -> bool:
    """Check if a column exists in a table.

    :param conn: Active database connection.
    :param table: Table name.
    :param column: Column name to check.
    :return: True if the column exists.
    """
    inspector = inspect(conn)  # type: ignore[arg-type]
    columns = [c["name"] for c in inspector.get_columns(table)]
    return column in columns


def upgrade() -> None:
    """Rename bibtext_id → bibtex_id in papers and bib if the typo variant exists."""
    conn = op.get_bind()

    # Handle papers table
    if _has_column(conn, "papers", "bibtext_id") and not _has_column(conn, "papers", "bibtex_id"):
        # Drop FK if it exists (best-effort)
        try:
            op.drop_constraint("fk_bibtex_id", "papers", type_="foreignkey")
        except Exception:
            pass
        op.alter_column("papers", "bibtext_id", new_column_name="bibtex_id")
        # Re-create FK constraint
        op.create_foreign_key(
            "fk_bibtex_id", "papers", "bib", ["bibtex_id"], ["bibtex_id"]
        )

    # Handle bib table
    if _has_column(conn, "bib", "bibtext_id") and not _has_column(conn, "bib", "bibtex_id"):
        op.alter_column("bib", "bibtext_id", new_column_name="bibtex_id")


def downgrade() -> None:
    """Rename bibtex_id → bibtext_id (reversing the typo correction)."""
    conn = op.get_bind()

    if _has_column(conn, "papers", "bibtex_id"):
        try:
            op.drop_constraint("fk_bibtex_id", "papers", type_="foreignkey")
        except Exception:
            pass
        op.alter_column("papers", "bibtex_id", new_column_name="bibtext_id")

    if _has_column(conn, "bib", "bibtex_id"):
        op.alter_column("bib", "bibtex_id", new_column_name="bibtext_id")
