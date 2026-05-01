"""Legacy ``bibtext_id`` column rename — converges historical and modern DBs.

Targets databases written by the historical ``paper_sorts/add.py``,
``paper_sorts/get_data.py``, or ``paper_sorts/search.py`` scripts, which
used the column name ``bibtext_id`` (sic — note the misspelling) on both
``papers`` and ``bib``. Modern databases (those written by the OO
``DatabaseConnector`` stack) already use ``bibtex_id`` and are
converged on no-op.

Revision ID: 002
Revises: 001
Create Date: 2026-05-01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "002"
down_revision: str | None = "001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Rename ``bibtext_id`` -> ``bibtex_id`` on ``papers`` and ``bib`` if present.

    Idempotent — already-modern databases (``bibtex_id`` column already
    in place) are left untouched. The single foreign key
    ``papers.bibtex_id -> bib.bibtex_id`` is dropped before the rename
    and recreated under the canonical name ``fk_bibtex_id`` so legacy
    auto-generated FK names converge to the contract.
    """
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if not insp.has_table("papers") or not insp.has_table("bib"):
        return

    papers_cols = {c["name"] for c in insp.get_columns("papers")}
    bib_cols = {c["name"] for c in insp.get_columns("bib")}

    needs_papers_rename = "bibtext_id" in papers_cols and "bibtex_id" not in papers_cols
    needs_bib_rename = "bibtext_id" in bib_cols and "bibtex_id" not in bib_cols

    if not needs_papers_rename and not needs_bib_rename:
        return

    for fk in insp.get_foreign_keys("papers"):
        name = fk.get("name")
        if name:
            op.drop_constraint(name, "papers", type_="foreignkey")

    if needs_bib_rename:
        op.alter_column("bib", "bibtext_id", new_column_name="bibtex_id")
    if needs_papers_rename:
        op.alter_column("papers", "bibtext_id", new_column_name="bibtex_id")

    op.create_foreign_key(
        "fk_bibtex_id",
        "papers",
        "bib",
        ["bibtex_id"],
        ["bibtex_id"],
    )


def downgrade() -> None:
    """Downgrade is intentionally one-way; reversal requires a backup restore.

    Renaming ``bibtex_id`` back to ``bibtext_id`` is mechanically a single
    ALTER, but doing so without also reverting the application code (which
    only knows ``bibtex_id``) makes any rows written after the upgrade
    unreachable to the legacy queries. The safe rollback is to restore
    from a pre-migration backup.
    """
    msg = (
        "Migration 002 is one-way: bibtext_id -> bibtex_id rename cannot be "
        "reversed without data loss. Restore from a pre-migration backup "
        "if rollback is required."
    )
    raise NotImplementedError(msg)
