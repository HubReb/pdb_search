"""Initial schema: verbatim port of the legacy DDL.

Revision ID: 001_initial_schema
Revises:
Create Date: 2026-06-15

Creates the four original tables: papers, bib, authors_id, authors_papers.
Schema-preservation rules: no NOT NULL outside primary keys, no DDL FK on
authors_papers, no extra indexes.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "001_initial_schema"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create all four tables in the canonical schema."""
    op.create_table(
        "bib",
        sa.Column("bibtex_id", sa.String(), nullable=False),
        sa.Column("bibtex", sa.Text(), nullable=True, unique=True),
        sa.PrimaryKeyConstraint("bibtex_id"),
    )
    op.create_table(
        "papers",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("title", sa.String(), nullable=True),
        sa.Column("contents", sa.Text(), nullable=True),
        sa.Column("bibtex_id", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "authors_id",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("author", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "authors_papers",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("author_id", sa.Integer(), nullable=True),
        sa.Column("paper_id", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        # No FK constraints — schema preservation rule
    )


def downgrade() -> None:
    """Drop all four tables."""
    op.drop_table("authors_papers")
    op.drop_table("authors_id")
    op.drop_table("papers")
    op.drop_table("bib")
