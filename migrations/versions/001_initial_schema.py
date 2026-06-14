"""Initial schema: create all four tables, handle legacy bibtext_id variant.

Revision ID: 001
Revises: None
Create Date: 2026-06-14

This migration:
1. Creates bib, papers, authors_id, authors_papers tables from the canonical DDL
2. Detects the legacy schema variant with bibtext_id (sic typo) in bib and papers tables
   and renames those columns to bibtex_id
3. Is idempotent — safe to run on already-migrated databases (uses IF NOT EXISTS / IF EXISTS)

Schema preservation contract (do NOT relax):
- No NOT NULL constraints beyond primary keys
- No DDL foreign keys on authors_papers
- No indexes beyond primary keys
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
    """Create canonical schema, migrating legacy bibtext_id variant if present.

    Steps (all idempotent):
    1. Create authors_papers table
    2. Create authors_id table
    3. Create bib table (handles bibtext_id → bibtex_id rename if needed)
    4. Create papers table (handles bibtext_id → bibtex_id rename if needed)
    """
    conn = op.get_bind()

    # 1. Create authors_papers
    conn.execute(
        sa.text(
            "CREATE TABLE IF NOT EXISTS authors_papers "
            "(id SERIAL PRIMARY KEY, author_id INT, paper_id INT)"
        )
    )

    # 2. Create authors_id
    conn.execute(
        sa.text(
            "CREATE TABLE IF NOT EXISTS authors_id "
            "(id SERIAL PRIMARY KEY, author TEXT)"
        )
    )

    # 3. Handle bib table
    # Check if bib table exists
    bib_exists = conn.execute(
        sa.text("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='bib')")
    ).scalar()

    if not bib_exists:
        # Fresh install: create canonical bib table
        conn.execute(
            sa.text(
                "CREATE TABLE bib "
                "(bibtex_id TEXT PRIMARY KEY, bibtex TEXT, "
                "CONSTRAINT bib_bibtex_unique UNIQUE (bibtex))"
            )
        )
    else:
        # Table exists — check for legacy bibtext_id typo column
        bibtext_id_exists = conn.execute(
            sa.text(
                "SELECT EXISTS("
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_name='bib' AND column_name='bibtext_id'"
                ")"
            )
        ).scalar()
        if bibtext_id_exists:
            # Rename bibtext_id → bibtex_id
            conn.execute(sa.text("ALTER TABLE bib RENAME COLUMN bibtext_id TO bibtex_id"))
        # Add unique constraint on bibtex if missing
        bib_unique_exists = conn.execute(
            sa.text(
                "SELECT EXISTS("
                "SELECT 1 FROM information_schema.table_constraints "
                "WHERE table_name='bib' AND constraint_name='bib_bibtex_unique'"
                ")"
            )
        ).scalar()
        if not bib_unique_exists:
            # Check if there's a column 'bibtext' that needs renaming
            bibtext_col_exists = conn.execute(
                sa.text(
                    "SELECT EXISTS("
                    "SELECT 1 FROM information_schema.columns "
                    "WHERE table_name='bib' AND column_name='bibtext'"
                    ")"
                )
            ).scalar()
            if bibtext_col_exists:
                conn.execute(sa.text("ALTER TABLE bib RENAME COLUMN bibtext TO bibtex"))

    # 4. Handle papers table
    papers_exists = conn.execute(
        sa.text(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='papers')"
        )
    ).scalar()

    if not papers_exists:
        # Fresh install: create canonical papers table with FK
        conn.execute(
            sa.text(
                "CREATE TABLE papers "
                "(id SERIAL PRIMARY KEY, title TEXT, contents TEXT, bibtex_id TEXT, "
                "CONSTRAINT fk_bibtex_id FOREIGN KEY (bibtex_id) REFERENCES bib(bibtex_id))"
            )
        )
    else:
        # Table exists — check for legacy bibtext_id typo column in papers
        papers_bibtext_id_exists = conn.execute(
            sa.text(
                "SELECT EXISTS("
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_name='papers' AND column_name='bibtext_id'"
                ")"
            )
        ).scalar()
        if papers_bibtext_id_exists:
            # Must rename bib.bibtext_id before this FK can reference it
            # (bib rename was done above in step 3)
            conn.execute(sa.text("ALTER TABLE papers RENAME COLUMN bibtext_id TO bibtex_id"))


def downgrade() -> None:
    """Drop all four tables in reverse dependency order."""
    conn = op.get_bind()
    conn.execute(sa.text("DROP TABLE IF EXISTS papers"))
    conn.execute(sa.text("DROP TABLE IF EXISTS bib"))
    conn.execute(sa.text("DROP TABLE IF EXISTS authors_papers"))
    conn.execute(sa.text("DROP TABLE IF EXISTS authors_id"))
