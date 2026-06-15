"""Converge the legacy ``bibtext_id``/``bibtext`` typo columns onto canonical names.

Databases created by the older procedural modules (``get_data.py`` / ``add.py``) used the
misspelled ``bibtext_id`` column on both ``papers`` and ``bib`` (and a ``bibtext`` data column
on ``bib``). This revision renames them to the canonical ``bibtex_id`` / ``bibtex`` if and only
if the legacy names are present, so it is idempotent: on an already-canonical database it is a
no-op, and a rerun after convergence does nothing.

No rows are added, removed, or rewritten — only column names change — so paper / author /
authorship / bib counts are preserved exactly.

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


def _columns(table: str) -> set[str]:
    """Return the set of column names on a table.

    :param table: the table to inspect.
    :return: the column names present.
    """
    bind = op.get_bind()
    inspector = inspect(bind)
    return {col["name"] for col in inspector.get_columns(table)}


def upgrade() -> None:
    """Rename legacy typo columns to canonical names where present (idempotent)."""
    bib_columns = _columns("bib")
    if "bibtext_id" in bib_columns and "bibtex_id" not in bib_columns:
        op.alter_column("bib", "bibtext_id", new_column_name="bibtex_id")
    if "bibtext" in bib_columns and "bibtex" not in bib_columns:
        op.alter_column("bib", "bibtext", new_column_name="bibtex")

    paper_columns = _columns("papers")
    if "bibtext_id" in paper_columns and "bibtex_id" not in paper_columns:
        op.alter_column("papers", "bibtext_id", new_column_name="bibtex_id")


def downgrade() -> None:
    """Rename canonical columns back to the legacy typo names (best-effort)."""
    bib_columns = _columns("bib")
    if "bibtex_id" in bib_columns and "bibtext_id" not in bib_columns:
        op.alter_column("bib", "bibtex_id", new_column_name="bibtext_id")
    if "bibtex" in bib_columns and "bibtext" not in bib_columns:
        op.alter_column("bib", "bibtex", new_column_name="bibtext")

    paper_columns = _columns("papers")
    if "bibtex_id" in paper_columns and "bibtext_id" not in paper_columns:
        op.alter_column("papers", "bibtex_id", new_column_name="bibtext_id")
