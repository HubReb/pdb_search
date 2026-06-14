"""Initial schema — verbatim port of DatabaseConnector.create_tables() DDL.

Revision ID: 001
Revises:
Create Date: 2026-06-14
"""

from alembic import op

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create all four tables as in the original schema.

    Schema-preservation contract: no NOT NULL outside PKs, no DDL FKs on
    authors_papers, no extra indexes beyond primary keys.
    """
    op.execute("""
        CREATE TABLE IF NOT EXISTS bib (
            bibtex_id TEXT PRIMARY KEY,
            bibtex TEXT UNIQUE
        )
    """)
    op.execute("""
        CREATE TABLE IF NOT EXISTS papers (
            id SERIAL PRIMARY KEY,
            title TEXT,
            contents TEXT,
            bibtex_id TEXT REFERENCES bib(bibtex_id)
        )
    """)
    op.execute("""
        CREATE TABLE IF NOT EXISTS authors_id (
            id SERIAL PRIMARY KEY,
            author TEXT
        )
    """)
    op.execute("""
        CREATE TABLE IF NOT EXISTS authors_papers (
            id SERIAL PRIMARY KEY,
            author_id INTEGER,
            paper_id INTEGER
        )
    """)


def downgrade() -> None:
    """Drop all four tables in reverse dependency order."""
    op.execute("DROP TABLE IF EXISTS authors_papers")
    op.execute("DROP TABLE IF EXISTS papers")
    op.execute("DROP TABLE IF EXISTS authors_id")
    op.execute("DROP TABLE IF EXISTS bib")
