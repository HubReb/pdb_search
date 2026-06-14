"""Converge legacy bibtext_id typo to canonical bibtex_id.

Revision ID: 002_converge_bibtext_typo
Revises: 001_initial_schema
Create Date: 2026-06-15

If the papers table has a column named 'bibtext_id' (legacy typo from
add.py / get_data.py), rename it to 'bibtex_id'. Idempotent: no-op if
'bibtex_id' already exists or 'bibtext_id' is not present.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "002_converge_bibtext_typo"
down_revision: Union[str, None] = "001_initial_schema"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _get_column_names(connection: sa.engine.Connection, table_name: str) -> list[str]:
    """Return column names for the given table.

    :param connection: active database connection
    :param table_name: name of the table to inspect
    :return: list of column name strings
    """
    inspector = inspect(connection)
    columns = inspector.get_columns(table_name)
    return [col["name"] for col in columns]


def upgrade() -> None:
    """Rename bibtext_id → bibtex_id in papers table if the typo column exists."""
    bind = op.get_bind()
    col_names = _get_column_names(bind, "papers")
    if "bibtext_id" in col_names and "bibtex_id" not in col_names:
        op.alter_column(
            "papers",
            "bibtext_id",
            new_column_name="bibtex_id",
            existing_type=sa.String(),
            existing_nullable=True,
        )


def downgrade() -> None:
    """Rename bibtex_id → bibtext_id (restores legacy typo) if needed."""
    bind = op.get_bind()
    col_names = _get_column_names(bind, "papers")
    if "bibtex_id" in col_names and "bibtext_id" not in col_names:
        op.alter_column(
            "papers",
            "bibtex_id",
            new_column_name="bibtext_id",
            existing_type=sa.String(),
            existing_nullable=True,
        )
