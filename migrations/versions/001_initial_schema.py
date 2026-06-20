"""Initial schema — verbatim port of DatabaseConnector.create_tables().

Revision ID: 001
Revises:
Create Date: 2026-06-20

Creates the four tables as defined in the legacy DatabaseConnector:

- ``authors_papers`` — many-to-many link (no DDL FKs, preserved from legacy)
- ``authors_id`` — author name table
- ``bib`` — BibTeX entries, keyed by citation key
- ``papers`` — paper metadata, FK -> bib.bibtex_id
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    """Create all four tables.

    Matches the DDL produced by ``DatabaseConnector.create_tables()`` in the
    legacy codebase.  No NOT NULL constraints beyond primary keys.  No DDL
    foreign keys on ``authors_papers``.
    """
    op.create_table(
        "authors_papers",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("author_id", sa.Integer),
        sa.Column("paper_id", sa.Integer),
    )
    op.create_table(
        "authors_id",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("author", sa.Text),
    )
    op.create_table(
        "bib",
        sa.Column("bibtex_id", sa.Text, primary_key=True),
        sa.Column("bibtex", sa.Text, unique=True),
    )
    op.create_table(
        "papers",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("title", sa.Text),
        sa.Column("contents", sa.Text),
        sa.Column(
            "bibtex_id",
            sa.String,
            sa.ForeignKey("bib.bibtex_id"),
        ),
    )


def downgrade():
    """Drop all four tables in reverse dependency order."""
    op.drop_table("papers")
    op.drop_table("bib")
    op.drop_table("authors_id")
    op.drop_table("authors_papers")
