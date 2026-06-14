"""Initial schema — verbatim DDL port of DatabaseConnector.create_tables().

Revision ID: 001
Revises:
Create Date: 2026-06-14
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create the four tables if they do not already exist.

    Verbatim port of ``DatabaseConnector.create_tables()`` using ``IF NOT
    EXISTS`` so this migration is idempotent (safe to re-run).
    """
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS authors_papers (
            id SERIAL PRIMARY KEY,
            author_id INT,
            paper_id INT
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS authors_id (
            id SERIAL PRIMARY KEY,
            author TEXT
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS bib (
            bibtex_id TEXT PRIMARY KEY,
            bibtex TEXT,
            CONSTRAINT uq_bib_bibtex UNIQUE (bibtex)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS papers (
            id SERIAL PRIMARY KEY,
            title TEXT,
            contents TEXT,
            bibtex_id TEXT,
            CONSTRAINT fk_bibtex_id FOREIGN KEY (bibtex_id) REFERENCES bib(bibtex_id)
        )
        """
    )


def downgrade() -> None:
    """Drop the four tables in reverse dependency order."""
    op.execute("DROP TABLE IF EXISTS papers")
    op.execute("DROP TABLE IF EXISTS bib")
    op.execute("DROP TABLE IF EXISTS authors_papers")
    op.execute("DROP TABLE IF EXISTS authors_id")
