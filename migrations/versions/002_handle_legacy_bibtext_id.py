"""Idempotent guard for legacy bibtext_id typo column in papers table.

Revision ID: 002
Revises: 001
Create Date: 2026-06-14

Some personal databases were created by the legacy add.py / search.py /
get_data.py modules which used the typo column name 'bibtext_id' (note the
transposed 'x' and 't').  This migration renames it to 'bibtex_id' if present,
or does nothing if the canonical name is already in use.

Idempotency guarantee: safe to run multiple times.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _column_exists(table: str, column: str) -> bool:
    """Return True if the named column exists in the given table.

    Args:
        table: Table name to inspect.
        column: Column name to look for.

    Returns:
        True if the column exists; False otherwise.
    """
    conn = op.get_bind()
    result = conn.execute(
        sa.text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = :tbl AND column_name = :col"
        ),
        {"tbl": table, "col": column},
    )
    return result.fetchone() is not None


def upgrade() -> None:
    """Rename bibtext_id → bibtex_id in papers if the typo column exists."""
    if _column_exists("papers", "bibtext_id") and not _column_exists("papers", "bibtex_id"):
        op.alter_column("papers", "bibtext_id", new_column_name="bibtex_id")


def downgrade() -> None:
    """Rename bibtex_id → bibtext_id (reverses the typo fix)."""
    if _column_exists("papers", "bibtex_id") and not _column_exists("papers", "bibtext_id"):
        op.alter_column("papers", "bibtex_id", new_column_name="bibtext_id")
