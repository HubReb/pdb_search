"""Converge a legacy ``bibtext_id`` (sic) schema onto the canonical one.

The procedural legacy importer (``get_data.py``) created tables with the typo
columns ``bibtext_id`` and ``bibtext`` instead of ``bibtex_id``/``bibtex``. This
revision renames those columns onto the canonical names. Every step inspects the
live schema first, so the migration is idempotent: a database already on the
canonical schema (including a fresh one created by revision 0001) is untouched,
a partially-renamed database is completed, and a rerun is a no-op. No rows are
created or deleted, so all counts are preserved (FR-011 / US4).

Revision ID: 0002
Revises: 0001
Create Date: 2026-06-17

"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
from sqlalchemy import inspect

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _columns(table: str) -> set[str]:
    """Return the set of column names present on ``table`` in the live DB.

    :param table: the table to inspect.
    :returns: the column names, or an empty set if the table is absent.
    """
    inspector = inspect(op.get_bind())
    if table not in inspector.get_table_names():
        return set()
    return {col["name"] for col in inspector.get_columns(table)}


def _rename_if_needed(table: str, old: str, new: str) -> None:
    """Rename ``old`` → ``new`` on ``table`` only when that is the live state.

    :param table: the table to operate on.
    :param old: the legacy column name to rename from.
    :param new: the canonical column name to rename to.
    """
    cols = _columns(table)
    if old in cols and new not in cols:
        op.alter_column(table, old, new_column_name=new)


def upgrade() -> None:
    """Rename legacy typo columns onto canonical names, idempotently."""
    _rename_if_needed("bib", "bibtext_id", "bibtex_id")
    _rename_if_needed("bib", "bibtext", "bibtex")
    _rename_if_needed("papers", "bibtext_id", "bibtex_id")


def downgrade() -> None:
    """Rename canonical columns back to the legacy typo names, idempotently."""
    _rename_if_needed("papers", "bibtex_id", "bibtext_id")
    _rename_if_needed("bib", "bibtex", "bibtext")
    _rename_if_needed("bib", "bibtex_id", "bibtext_id")
