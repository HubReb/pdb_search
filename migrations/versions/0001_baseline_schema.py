"""Baseline schema — verbatim port of the original DDL.

Creates the four canonical tables (``bib``, ``papers``, ``authors_id``,
``authors_papers``) exactly as the legacy ``DatabaseConnector.create_tables``
produced them: canonical ``bibtex_id`` column, ``bib.bibtex`` UNIQUE, FK
``papers.bibtex_id → bib.bibtex_id``, and a bare ``authors_papers`` link table
with no foreign keys.

Revision ID: 0001
Revises:
Create Date: 2026-06-17

"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the four canonical tables."""
    op.create_table(
        "bib",
        sa.Column("bibtex_id", sa.Text(), primary_key=True),
        sa.Column("bibtex", sa.Text(), unique=True),
    )
    op.create_table(
        "papers",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("title", sa.Text()),
        sa.Column("contents", sa.Text()),
        sa.Column("bibtex_id", sa.Text()),
        sa.ForeignKeyConstraint(
            ["bibtex_id"], ["bib.bibtex_id"], name="fk_bibtex_id"
        ),
    )
    op.create_table(
        "authors_id",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("author", sa.Text()),
    )
    op.create_table(
        "authors_papers",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("author_id", sa.Integer()),
        sa.Column("paper_id", sa.Integer()),
    )


def downgrade() -> None:
    """Drop the four tables in reverse dependency order."""
    op.drop_table("authors_papers")
    op.drop_table("authors_id")
    op.drop_table("papers")
    op.drop_table("bib")
