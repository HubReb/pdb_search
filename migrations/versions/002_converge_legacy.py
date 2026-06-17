"""converge legacy bibtext_id (sic) columns onto canonical bibtex_id

Revision ID: 002
Revises: 001
Create Date: 2026-06-17

Idempotent convergence: the older procedural modules used the column name
``bibtext_id`` (sic) on ``papers`` and ``bib``. This revision renames those
columns to the canonical ``bibtex_id`` **only if** the legacy column is present,
guarded on ``information_schema`` so a fresh or already-canonical database is a
no-op. Renaming preserves every row, so paper/author/authorship/bib counts are
unchanged.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

revision: str = "002"
down_revision: str | None = "001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_column(table: str, column: str) -> bool:
    """Return whether ``table.column`` exists in the current database.

    :param table: table name.
    :param column: column name.
    :return: ``True`` if the column exists.
    """
    bind = op.get_bind()
    result = bind.execute(
        text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = :t AND column_name = :c"
        ),
        {"t": table, "c": column},
    ).first()
    return result is not None


def upgrade() -> None:
    """Rename legacy ``bibtext_id`` columns to ``bibtex_id`` where present."""
    for table in ("papers", "bib"):
        if _has_column(table, "bibtext_id") and not _has_column(table, "bibtex_id"):
            op.alter_column(table, "bibtext_id", new_column_name="bibtex_id")
    # The legacy bib source column was named ``bibtext`` (sic); converge it too.
    if _has_column("bib", "bibtext") and not _has_column("bib", "bibtex"):
        op.alter_column("bib", "bibtext", new_column_name="bibtex")


def downgrade() -> None:
    """Rename canonical columns back to the legacy ``bibtext_id`` form."""
    for table in ("papers", "bib"):
        if _has_column(table, "bibtex_id") and not _has_column(table, "bibtext_id"):
            op.alter_column(table, "bibtex_id", new_column_name="bibtext_id")
    if _has_column("bib", "bibtex") and not _has_column("bib", "bibtext"):
        op.alter_column("bib", "bibtex", new_column_name="bibtext")
