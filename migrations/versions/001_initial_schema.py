"""initial schema — verbatim port of the legacy canonical DDL

Revision ID: 001_initial
Revises:
Create Date: 2026-06-19

Reproduces the four tables exactly as the legacy ``DatabaseConnector.create_tables``
created them: ``bib(bibtex_id PK, bibtex UNIQUE)``, ``papers(id, title, contents,
bibtex_id FK -> bib)``, ``authors_id(id, author)``, ``authors_papers(id, author_id,
paper_id)`` with no foreign keys. No NOT NULL outside primary keys, no indexes
beyond the primary keys and the single UNIQUE constraint.
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
    op.create_table(
        "bib",
        sa.Column("bibtex_id", sa.Text(), primary_key=True),
        sa.Column("bibtex", sa.Text()),
        sa.UniqueConstraint("bibtex", name="bib_bibtex_key"),
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
    op.drop_table("authors_papers")
    op.drop_table("authors_id")
    op.drop_table("papers")
    op.drop_table("bib")
