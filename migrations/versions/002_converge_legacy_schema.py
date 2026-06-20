"""Convergence migration — rename legacy typo columns.

Handles two historical schema variants:
- Newer (database_connector.py): bibtex_id / bibtex  (canonical)
- Older (get_data.py / add.py / search.py): bibtext_id / bibtext  (typo)

If the typo columns are detected, they are renamed to the canonical names.
If the canonical columns already exist, this migration is a no-op.

Revision ID: 002
Revises: 001
Create Date: 2026-06-20
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _column_exists(conn: sa.engine.Connection, table: str, column: str) -> bool:
    """Return True if *column* exists in *table*.

    :param conn: Active database connection.
    :param table: Table name to inspect.
    :param column: Column name to check for.
    :returns: ``True`` if the column exists, ``False`` otherwise.
    """
    result = conn.execute(
        sa.text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = :table AND column_name = :column"
        ),
        {"table": table, "column": column},
    )
    return result.fetchone() is not None


def upgrade() -> None:
    """Rename typo columns to canonical names if they exist.

    Idempotent: checks for column existence before renaming.
    """
    conn = op.get_bind()

    # Fix papers table: bibtext_id -> bibtex_id
    if _column_exists(conn, "papers", "bibtext_id") and not _column_exists(
        conn, "papers", "bibtex_id"
    ):
        conn.execute(
            sa.text("ALTER TABLE papers RENAME COLUMN bibtext_id TO bibtex_id;")
        )

    # Fix bib table: bibtext_id -> bibtex_id
    if _column_exists(conn, "bib", "bibtext_id") and not _column_exists(
        conn, "bib", "bibtex_id"
    ):
        conn.execute(
            sa.text("ALTER TABLE bib RENAME COLUMN bibtext_id TO bibtex_id;")
        )

    # Fix bib table: bibtext -> bibtex
    if _column_exists(conn, "bib", "bibtext") and not _column_exists(
        conn, "bib", "bibtex"
    ):
        conn.execute(
            sa.text("ALTER TABLE bib RENAME COLUMN bibtext TO bibtex;")
        )


def downgrade() -> None:
    """Rename canonical columns back to legacy typo names.

    Only renames if the canonical name exists (reverse of upgrade).
    """
    conn = op.get_bind()

    # Revert papers table: bibtex_id -> bibtext_id
    if _column_exists(conn, "papers", "bibtex_id") and not _column_exists(
        conn, "papers", "bibtext_id"
    ):
        conn.execute(
            sa.text("ALTER TABLE papers RENAME COLUMN bibtex_id TO bibtext_id;")
        )

    # Revert bib table: bibtex_id -> bibtext_id
    if _column_exists(conn, "bib", "bibtex_id") and not _column_exists(
        conn, "bib", "bibtext_id"
    ):
        conn.execute(
            sa.text("ALTER TABLE bib RENAME COLUMN bibtex_id TO bibtext_id;")
        )

    # Revert bib table: bibtex -> bibtext
    if _column_exists(conn, "bib", "bibtex") and not _column_exists(
        conn, "bib", "bibtext"
    ):
        conn.execute(
            sa.text("ALTER TABLE bib RENAME COLUMN bibtex TO bibtext;")
        )
