"""Initial schema: verbatim port of the legacy DatabaseConnector.create_tables() DDL.

Creates the four core tables with the canonical column name ``bibtex_id``
(not the legacy typo ``bibtext_id`` used by add.py / get_data.py — that
variant is handled by revision 002).

Revision ID: 001
Revises: (none)
Create Date: 2026-04-26
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

# Revision identifiers used by Alembic.
revision: str = "001"
down_revision: str | None = None
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    """Create all four tables in dependency order.

    Table creation order respects the FK from papers.bibtex_id → bib.bibtex_id.
    No NOT NULL constraints are added outside primary keys, and no DDL foreign
    keys are added to authors_papers — this preserves the original schema contract.
    """
    # authors_papers — many-to-many link, no DDL FKs (schema preservation rule)
    op.create_table(
        "authors_papers",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("author_id", sa.Integer, nullable=True),
        sa.Column("paper_id", sa.Integer, nullable=True),
    )

    # authors_id — author name registry
    op.create_table(
        "authors_id",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("author", sa.Text, nullable=True),
    )

    # bib — BibTeX entries; bibtex must be unique (enforced by legacy schema)
    op.create_table(
        "bib",
        sa.Column("bibtex_id", sa.Text, primary_key=True),
        sa.Column("bibtex", sa.Text, nullable=False, unique=True),
    )

    # papers — publication records with FK into bib
    op.create_table(
        "papers",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("title", sa.Text, nullable=True),
        sa.Column("contents", sa.Text, nullable=True),
        sa.Column("bibtex_id", sa.Text, sa.ForeignKey("bib.bibtex_id"), nullable=True),
    )


def downgrade() -> None:
    """Drop all four tables in reverse FK dependency order."""
    op.drop_table("papers")
    op.drop_table("bib")
    op.drop_table("authors_id")
    op.drop_table("authors_papers")
