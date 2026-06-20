"""converge legacy bibtext_id/bibtext typo columns to canonical names

Revision ID: 002_converge
Revises: 001_initial
Create Date: 2026-06-19

The older procedural modules created the typo columns ``bib.bibtext_id`` /
``bib.bibtext`` and ``papers.bibtext_id``. This revision renames them to the
canonical ``bibtex_id`` / ``bibtex`` **only when present**, guarded on
``information_schema.columns`` so the upgrade is idempotent: a no-op on an
already-canonical database, a converger on a legacy-typo one. It runs inside the
Alembic transaction, so a mid-run failure leaves the pre-migration state intact.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

revision: str = "002_converge"
down_revision: str | None = "001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_column(table: str, column: str) -> bool:
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
    if _has_column("bib", "bibtext_id"):
        op.alter_column("bib", "bibtext_id", new_column_name="bibtex_id")
    if _has_column("bib", "bibtext"):
        op.alter_column("bib", "bibtext", new_column_name="bibtex")
    if _has_column("papers", "bibtext_id"):
        op.alter_column("papers", "bibtext_id", new_column_name="bibtex_id")


def downgrade() -> None:
    if _has_column("papers", "bibtex_id"):
        op.alter_column("papers", "bibtex_id", new_column_name="bibtext_id")
    if _has_column("bib", "bibtex"):
        op.alter_column("bib", "bibtex", new_column_name="bibtext")
    if _has_column("bib", "bibtex_id"):
        op.alter_column("bib", "bibtex_id", new_column_name="bibtext_id")
