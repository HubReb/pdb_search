"""Fix legacy 'bibtext_id' column name typo.

The legacy procedural modules (add.py, get_data.py, search.py) used the
misspelled column name 'bibtext_id' instead of 'bibtex_id'. This migration
detects and renames the typo column to the canonical name in both the 'papers'
and 'bib' tables (where the legacy code also had 'bibtext' instead of 'bibtex').

This migration is idempotent: it checks column existence before renaming.
Running it on a database that already uses the canonical column names is safe.

Revision ID: 002
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import inspect


def _has_column(table: str, column: str) -> bool:
    """Check whether a column exists on a table using the bound connection.

    :param table: Table name to inspect.
    :param column: Column name to look for.
    :return: True if the column exists.
    """
    bind = op.get_bind()
    inspector = inspect(bind)
    columns = [c["name"] for c in inspector.get_columns(table)]
    return column in columns


# revision identifiers, used by Alembic
revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Rename legacy typo columns to canonical names if they exist.

    Handles two variants:
    - papers.bibtext_id → papers.bibtex_id
    - bib.bibtext_id → bib.bibtex_id  (older schema variant)
    - bib.bibtext → bib.bibtex  (older schema had 'bibtext' not 'bibtex')
    """
    # Fix papers.bibtext_id → papers.bibtex_id
    if _has_column("papers", "bibtext_id") and not _has_column("papers", "bibtex_id"):
        op.alter_column("papers", "bibtext_id", new_column_name="bibtex_id")

    # Fix bib.bibtext_id → bib.bibtex_id (if primary key column was misspelled)
    if _has_column("bib", "bibtext_id") and not _has_column("bib", "bibtex_id"):
        op.alter_column("bib", "bibtext_id", new_column_name="bibtex_id")

    # Fix bib.bibtext → bib.bibtex (the value column)
    if _has_column("bib", "bibtext") and not _has_column("bib", "bibtex"):
        op.alter_column("bib", "bibtext", new_column_name="bibtex")


def downgrade() -> None:
    """Reverse the column renames (only if they were applied)."""
    if _has_column("papers", "bibtex_id") and not _has_column("papers", "bibtext_id"):
        op.alter_column("papers", "bibtex_id", new_column_name="bibtext_id")

    if _has_column("bib", "bibtex_id") and not _has_column("bib", "bibtext_id"):
        op.alter_column("bib", "bibtex_id", new_column_name="bibtext_id")

    if _has_column("bib", "bibtex") and not _has_column("bib", "bibtext"):
        op.alter_column("bib", "bibtex", new_column_name="bibtext")
