"""Create the canonical four-table schema.

This is a verbatim port of the legacy ``DatabaseConnector.create_tables()`` DDL: four tables,
the single ``papers.bibtex_id -> bib.bibtex_id`` foreign key (``fk_bibtex_id``), the
``bib.bibtex`` UNIQUE constraint, and no foreign keys on ``authors_papers``. No NOT NULL is
declared outside primary keys and no indexes beyond the primary keys are created.

Revision ID: 001
Revises:
Create Date: 2026-06-15

"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "001"
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
        sa.ForeignKeyConstraint(["bibtex_id"], ["bib.bibtex_id"], name="fk_bibtex_id"),
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
    """Drop the four canonical tables."""
    op.drop_table("authors_papers")
    op.drop_table("authors_id")
    op.drop_table("papers")
    op.drop_table("bib")
