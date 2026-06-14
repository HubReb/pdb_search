"""Fix legacy bibtext_id (sic) column name in papers table.

Some older databases used the misspelled column name `bibtext_id` instead
of the correct `bibtex_id`. This migration renames it if present. Idempotent.

Revision ID: 002
Revises: 001
Create Date: 2026-06-14
"""

from alembic import op
from sqlalchemy import inspect

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Rename bibtext_id → bibtex_id in papers if the old name exists.

    Safe to run on databases that already have the correct column name.
    """
    bind = op.get_bind()
    inspector = inspect(bind)
    columns = [col["name"] for col in inspector.get_columns("papers")]
    if "bibtext_id" in columns and "bibtex_id" not in columns:
        op.execute("ALTER TABLE papers RENAME COLUMN bibtext_id TO bibtex_id")


def downgrade() -> None:
    """Rename bibtex_id → bibtext_id (restores legacy misspelling).

    Note: Only reverses if bibtex_id exists; no-op otherwise.
    """
    bind = op.get_bind()
    inspector = inspect(bind)
    columns = [col["name"] for col in inspector.get_columns("papers")]
    if "bibtex_id" in columns and "bibtext_id" not in columns:
        op.execute("ALTER TABLE papers RENAME COLUMN bibtex_id TO bibtext_id")
