"""Converge legacy bibtext_id (typo) column to canonical bibtex_id.

Some personal databases were populated by the legacy ``get_data.py`` /
``add.py`` procedural modules, which used ``bibtext_id`` (note the transposed
``x`` and ``t``) instead of ``bibtex_id``.  This migration detects whether the
typo column exists in either the ``bib`` or ``papers`` table and renames it.

This migration is idempotent: if the canonical column already exists (i.e. the
database was populated by ``DatabaseConnector``), the rename steps are skipped.

Revision ID: 002
Revises: 001
Create Date: 2026-06-14
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_CHECK_COLUMN = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = :table AND column_name = :column
"""


def _column_exists(table: str, column: str) -> bool:
    """Return True if *column* exists in *table*."""
    conn = op.get_bind()
    result = conn.execute(
        op.get_context().dialect.text(  # type: ignore[union-attr]
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = :table AND column_name = :column"
        ),
        {"table": table, "column": column},
    )
    return result.fetchone() is not None


def upgrade() -> None:
    """Rename ``bibtext_id`` to ``bibtex_id`` in ``bib`` and ``papers`` if needed."""
    # Fix bib table: bibtext_id (PK typo) -> bibtex_id
    if _column_exists("bib", "bibtext_id"):
        # Rename the primary key column
        op.execute("ALTER TABLE bib DROP CONSTRAINT IF EXISTS bib_pkey")
        op.execute("ALTER TABLE bib RENAME COLUMN bibtext_id TO bibtex_id")
        op.execute("ALTER TABLE bib ADD PRIMARY KEY (bibtex_id)")
        # Rename the bibtex text column if needed (legacy used "bibtext" too)
        if _column_exists("bib", "bibtext"):
            op.execute("ALTER TABLE bib RENAME COLUMN bibtext TO bibtex")
            op.execute(
                "ALTER TABLE bib ADD CONSTRAINT IF NOT EXISTS uq_bib_bibtex UNIQUE (bibtex)"
            )

    # Fix papers table: bibtext_id FK typo -> bibtex_id
    if _column_exists("papers", "bibtext_id"):
        op.execute(
            "ALTER TABLE papers DROP CONSTRAINT IF EXISTS fk_bibtex_id"
        )
        op.execute("ALTER TABLE papers RENAME COLUMN bibtext_id TO bibtex_id")
        op.execute(
            "ALTER TABLE papers ADD CONSTRAINT fk_bibtex_id "
            "FOREIGN KEY (bibtex_id) REFERENCES bib(bibtex_id)"
        )


def downgrade() -> None:
    """Rename ``bibtex_id`` back to ``bibtext_id`` (restores typo schema)."""
    if _column_exists("papers", "bibtex_id"):
        op.execute("ALTER TABLE papers DROP CONSTRAINT IF EXISTS fk_bibtex_id")
        op.execute("ALTER TABLE papers RENAME COLUMN bibtex_id TO bibtext_id")

    if _column_exists("bib", "bibtex_id"):
        op.execute("ALTER TABLE bib DROP CONSTRAINT IF EXISTS bib_pkey")
        op.execute("ALTER TABLE bib RENAME COLUMN bibtex_id TO bibtext_id")
        op.execute("ALTER TABLE bib ADD PRIMARY KEY (bibtext_id)")

    if _column_exists("bib", "bibtex"):
        op.execute("ALTER TABLE bib RENAME COLUMN bibtex TO bibtext")
