"""converge legacy bibtext_id variant onto canonical bibtex_id

Older procedural modules created the schema with a misspelled ``bibtext_id``
column (on both ``bib`` and ``papers``). This revision converges such a
database onto the canonical ``bibtex_id`` spelling. It inspects the live
schema and only renames columns that are actually present, so it is idempotent:
on an already-canonical database it is a no-op, and a re-run after a partial
apply completes cleanly.

Revision ID: 002
Revises: 001
Create Date: 2026-06-15
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
from sqlalchemy import inspect

revision: str = "002"
down_revision: str | None = "001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _column_names(table: str) -> set[str]:
    """Return the set of column names currently present on a table."""
    inspector = inspect(op.get_bind())
    if table not in inspector.get_table_names():
        return set()
    return {col["name"] for col in inspector.get_columns(table)}


def upgrade() -> None:
    """Rename ``bibtext_id`` → ``bibtex_id`` on ``bib`` and ``papers`` if present."""
    for table in ("bib", "papers"):
        columns = _column_names(table)
        if "bibtext_id" in columns and "bibtex_id" not in columns:
            op.alter_column(table, "bibtext_id", new_column_name="bibtex_id")


def downgrade() -> None:
    """Rename ``bibtex_id`` → ``bibtext_id`` on ``bib`` and ``papers`` if present."""
    for table in ("bib", "papers"):
        columns = _column_names(table)
        if "bibtex_id" in columns and "bibtext_id" not in columns:
            op.alter_column(table, "bibtex_id", new_column_name="bibtext_id")
