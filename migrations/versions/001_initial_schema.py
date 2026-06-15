"""Initial canonical schema (verbatim port of the legacy DDL).

Reproduces the four tables created by the legacy
``DatabaseConnector.create_tables()`` exactly — no tightening (schema-
preservation contract): only primary keys are NOT NULL, ``authors_papers``
carries no foreign keys, the only FK is ``papers.bibtex_id -> bib.bibtex_id``,
and the only non-PK index is the ``bib.bibtex`` UNIQUE.

Revision ID: 001_initial
Revises:
Create Date: 2026-06-15
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the four canonical tables."""
    op.create_table(
        "bib",
        sa.Column("bibtex_id", sa.Text(), primary_key=True),
        sa.Column("bibtex", sa.Text(), nullable=True),
        sa.UniqueConstraint("bibtex", name="bib_bibtex_key"),
    )
    op.create_table(
        "papers",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("contents", sa.Text(), nullable=True),
        sa.Column("bibtex_id", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["bibtex_id"], ["bib.bibtex_id"], name="fk_bibtex_id"),
    )
    op.create_table(
        "authors_id",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("author", sa.Text(), nullable=True),
    )
    op.create_table(
        "authors_papers",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("author_id", sa.Integer(), nullable=True),
        sa.Column("paper_id", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    """Drop the four tables in dependency-safe order."""
    op.drop_table("authors_papers")
    op.drop_table("authors_id")
    op.drop_table("papers")
    op.drop_table("bib")
