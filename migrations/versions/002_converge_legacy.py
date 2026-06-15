"""Converge legacy ``bibtext_id`` (typo) databases onto the canonical schema.

Idempotent: every rename is guarded by an ``information_schema`` probe, so this
is a no-op on a database already in the canonical (``bibtex_id``) schema and on
a re-run after a successful convergence. Renames move no rows, so paper, author,
authorship, and bib counts are identical before and after (SC-004). Wrapped in
Alembic's transactional DDL, a mid-migration failure rolls the whole revision
back — never half-migrated (FR-011 AC-3).

Revision ID: 002_converge
Revises: 001_initial
Create Date: 2026-06-15
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
from sqlalchemy import inspect

revision: str = "002_converge"
down_revision: str | None = "001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _columns(table: str) -> set[str]:
    """Return the column names of a table via the live inspector.

    :param table: the table name to inspect.
    :returns: the set of column names (empty if the table is absent).
    """
    inspector = inspect(op.get_bind())
    if table not in inspector.get_table_names():
        return set()
    return {col["name"] for col in inspector.get_columns(table)}


def upgrade() -> None:
    """Rename legacy typo columns to canonical names, where present."""
    papers_cols = _columns("papers")
    if "bibtext_id" in papers_cols and "bibtex_id" not in papers_cols:
        op.alter_column("papers", "bibtext_id", new_column_name="bibtex_id")

    bib_cols = _columns("bib")
    if "bibtext_id" in bib_cols and "bibtex_id" not in bib_cols:
        op.alter_column("bib", "bibtext_id", new_column_name="bibtex_id")
    if "bibtext" in bib_cols and "bibtex" not in bib_cols:
        op.alter_column("bib", "bibtext", new_column_name="bibtex")


def downgrade() -> None:
    """Rename canonical columns back to the legacy typo names, where present."""
    papers_cols = _columns("papers")
    if "bibtex_id" in papers_cols and "bibtext_id" not in papers_cols:
        op.alter_column("papers", "bibtex_id", new_column_name="bibtext_id")

    bib_cols = _columns("bib")
    if "bibtex_id" in bib_cols and "bibtext_id" not in bib_cols:
        op.alter_column("bib", "bibtex_id", new_column_name="bibtext_id")
    if "bibtex" in bib_cols and "bibtext" not in bib_cols:
        op.alter_column("bib", "bibtex", new_column_name="bibtext")
