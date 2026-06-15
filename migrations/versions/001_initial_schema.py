"""Initial canonical schema for paper_sorts.

Creates the four tables in their canonical form:
  - bib (bibtex_id PK, bibtex UNIQUE)
  - papers (id PK, title, contents, bibtex_id FK → bib.bibtex_id)
  - authors_id (id PK, author)
  - authors_papers (id PK, author_id INT, paper_id INT) — no DDL FKs

Schema preservation contract:
  - No NOT NULL outside primary keys
  - No DDL foreign keys on authors_papers
  - No indexes beyond primary keys

Revision ID: 001
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic
revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create all four tables from scratch."""
    # bib table (must be created before papers due to FK)
    op.create_table(
        "bib",
        sa.Column("bibtex_id", sa.Text, primary_key=True),
        sa.Column("bibtex", sa.Text, nullable=True, unique=True),
    )

    # papers table
    op.create_table(
        "papers",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("title", sa.Text, nullable=True),
        sa.Column("contents", sa.Text, nullable=True),
        sa.Column(
            "bibtex_id",
            sa.Text,
            sa.ForeignKey("bib.bibtex_id"),
            nullable=True,
        ),
    )

    # authors_id table
    op.create_table(
        "authors_id",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("author", sa.Text, nullable=True),
    )

    # authors_papers table — intentionally NO DDL FKs (schema preservation contract)
    op.create_table(
        "authors_papers",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("author_id", sa.Integer, nullable=True),
        sa.Column("paper_id", sa.Integer, nullable=True),
    )


def downgrade() -> None:
    """Drop all four tables in reverse dependency order."""
    op.drop_table("authors_papers")
    op.drop_table("authors_id")
    op.drop_table("papers")
    op.drop_table("bib")
