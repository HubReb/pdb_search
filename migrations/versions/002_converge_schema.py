"""Converge legacy schema variants.

Revision ID: 002
Revises: 001
Create Date: 2026-06-20

Handles the ``bibtext_id`` typo column (missing 'a') used by the older
procedural modules (``add.py``, ``get_data.py``, ``search.py``) that
predate the ``DatabaseConnector`` OO stack.

If the ``papers`` table has a column named ``bibtext_id`` (the typo
variant), rename it to ``bibtex_id`` and ensure the FK to ``bib`` is
set up correctly.

If the column is already named ``bibtex_id`` (the canonical variant from
``DatabaseConnector``), this migration is a no-op.

Downgrade is a no-op — we never re-introduce the typo.
"""

from alembic import op
from sqlalchemy import inspect

# revision identifiers, used by Alembic.
revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def _papers_has_typo_column(conn):
    """Return True if ``papers.bibtext_id`` (typo) column exists.

    :param conn: Active database connection.
    :returns: Whether the legacy typo column is present.
    """
    inspector = inspect(conn)
    columns = [col["name"] for col in inspector.get_columns("papers")]
    return "bibtext_id" in columns


def upgrade():
    """Rename ``papers.bibtext_id`` to ``papers.bibtex_id`` if needed.

    Idempotent: checks whether the typo column exists before acting.
    """
    conn = op.get_bind()
    if not _papers_has_typo_column(conn):
        # Already on canonical schema — nothing to do.
        return

    # Rename the typo column.
    op.alter_column("papers", "bibtext_id", new_column_name="bibtex_id")

    # The FK constraint may have been dropped; add it back if needed.
    try:
        op.create_foreign_key(
            "fk_bibtex_id",
            "papers",
            "bib",
            ["bibtex_id"],
            ["bibtex_id"],
        )
    except Exception:  # noqa: BLE001
        # FK may already exist — safe to ignore.
        pass


def downgrade():
    """No-op — we never re-introduce the bibtext_id typo column."""
