"""Initial schema — verbatim port of DatabaseConnector.create_tables() DDL.

Revision ID: 001
Revises: (none)
Create Date: 2026-06-20
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision: str = "001"
down_revision: str | None = None
branch_labels: None = None
depends_on: None = None


def upgrade() -> None:
    """Create the four core tables matching the legacy create_tables() DDL exactly."""
    # authors_papers: many-to-many link; intentionally no DDL FK constraints
    op.create_table(
        "authors_papers",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("author_id", sa.Integer(), nullable=True),
        sa.Column("paper_id", sa.Integer(), nullable=True),
    )

    # authors_id: author name registry
    op.create_table(
        "authors_id",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("author", sa.Text(), nullable=True),
    )

    # bib: BibTeX entries keyed by unique citation key
    op.create_table(
        "bib",
        sa.Column("bibtex_id", sa.Text(), primary_key=True),
        sa.Column("bibtex", sa.Text(), nullable=False),
        sa.UniqueConstraint("bibtex", name="uq_bib_bibtex"),
    )

    # papers: publication metadata with FK into bib
    op.create_table(
        "papers",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("contents", sa.Text(), nullable=True),
        sa.Column("bibtex_id", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["bibtex_id"], ["bib.bibtex_id"], name="fk_bibtex_id"),
    )


def downgrade() -> None:
    """Drop the four core tables in reverse dependency order."""
    op.drop_table("papers")
    op.drop_table("bib")
    op.drop_table("authors_id")
    op.drop_table("authors_papers")
