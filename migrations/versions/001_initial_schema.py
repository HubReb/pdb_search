"""Initial schema — verbatim port of DatabaseConnector.create_tables().

Revision ID: 001
Revises:
Create Date: 2026-05-01

Creates the four tables that constitute the paper-sorts data model. The
shape is intentionally identical to the original ``create_tables()``
output: no NOT NULL outside primary keys, no FKs on ``authors_papers``,
the only declared FK is ``papers.bibtex_id -> bib.bibtex_id`` (named
``fk_bibtex_id`` per the original DDL), and the only non-PK UNIQUE is on
``bib.bibtex``. Each table is created only if absent so this revision is
a no-op against any already-modern personal database (acceptance criterion
in ``contracts/database-schema.md`` for FR-011 / SC-004).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not inspector.has_table("bib"):
        op.create_table(
            "bib",
            sa.Column("bibtex_id", sa.Text(), primary_key=True),
            sa.Column("bibtex", sa.Text(), nullable=True),
            sa.UniqueConstraint("bibtex"),
        )

    if not inspector.has_table("papers"):
        op.create_table(
            "papers",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("title", sa.Text(), nullable=True),
            sa.Column("contents", sa.Text(), nullable=True),
            sa.Column("bibtex_id", sa.Text(), nullable=True),
            sa.ForeignKeyConstraint(["bibtex_id"], ["bib.bibtex_id"], name="fk_bibtex_id"),
        )

    if not inspector.has_table("authors_id"):
        op.create_table(
            "authors_id",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("author", sa.Text(), nullable=True),
        )

    if not inspector.has_table("authors_papers"):
        op.create_table(
            "authors_papers",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("author_id", sa.Integer(), nullable=True),
            sa.Column("paper_id", sa.Integer(), nullable=True),
        )


def downgrade() -> None:
    op.drop_table("authors_papers")
    op.drop_table("authors_id")
    op.drop_table("papers")
    op.drop_table("bib")
