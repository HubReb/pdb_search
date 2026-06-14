"""Initial schema — verbatim port of four-table DDL from database_connector.py.

Revision ID: 001
Revises: (none)
Create Date: 2026-06-14

Creates the four original tables in the canonical schema:
  bib, papers, authors_id, authors_papers.

Schema-preservation contract: no NOT NULL beyond primary keys, no FK on
authors_papers, no new indexes.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create all four tables if they do not already exist."""
    # authors_papers — no DDL foreign keys (schema-preservation contract)
    op.create_table(
        "authors_papers",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("author_id", sa.Integer(), nullable=True),
        sa.Column("paper_id", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )

    # authors_id
    op.create_table(
        "authors_id",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("author", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )

    # bib — bibtex column UNIQUE matches original DDL
    op.create_table(
        "bib",
        sa.Column("bibtex_id", sa.Text(), nullable=False),
        sa.Column("bibtex", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("bibtex_id"),
        sa.UniqueConstraint("bibtex"),
    )

    # papers — FK to bib.bibtex_id
    op.create_table(
        "papers",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("contents", sa.Text(), nullable=True),
        sa.Column("bibtex_id", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["bibtex_id"], ["bib.bibtex_id"], name="fk_bibtex_id"),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    """Drop all four tables in reverse dependency order."""
    op.drop_table("papers")
    op.drop_table("bib")
    op.drop_table("authors_id")
    op.drop_table("authors_papers")
