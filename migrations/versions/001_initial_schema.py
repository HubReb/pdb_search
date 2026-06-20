"""Initial schema — four tables in canonical form.

Creates the four tables (authors_papers, authors_id, bib, papers) using
the canonical column names (bibtex_id, bibtex).  Uses CREATE TABLE IF NOT
EXISTS semantics so this revision is safe to apply against an existing
database that already has these tables.

Revision ID: 001
Revises: (none)
Create Date: 2026-06-20
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create all four tables if they do not already exist."""
    conn = op.get_bind()

    conn.execute(
        sa.text(
            "CREATE TABLE IF NOT EXISTS authors_papers ("
            "  id SERIAL PRIMARY KEY,"
            "  author_id INT,"
            "  paper_id INT"
            ");"
        )
    )
    conn.execute(
        sa.text(
            "CREATE TABLE IF NOT EXISTS authors_id ("
            "  id SERIAL PRIMARY KEY,"
            "  author TEXT"
            ");"
        )
    )
    conn.execute(
        sa.text(
            "CREATE TABLE IF NOT EXISTS bib ("
            "  bibtex_id TEXT PRIMARY KEY,"
            "  bibtex TEXT UNIQUE"
            ");"
        )
    )
    conn.execute(
        sa.text(
            "CREATE TABLE IF NOT EXISTS papers ("
            "  id SERIAL PRIMARY KEY,"
            "  title TEXT,"
            "  contents TEXT,"
            "  bibtex_id TEXT,"
            "  CONSTRAINT fk_bibtex_id FOREIGN KEY(bibtex_id) REFERENCES bib(bibtex_id)"
            ");"
        )
    )


def downgrade() -> None:
    """Drop all four tables in dependency order."""
    conn = op.get_bind()
    conn.execute(sa.text("DROP TABLE IF EXISTS authors_papers;"))
    conn.execute(sa.text("DROP TABLE IF EXISTS papers;"))
    conn.execute(sa.text("DROP TABLE IF EXISTS bib;"))
    conn.execute(sa.text("DROP TABLE IF EXISTS authors_id;"))
