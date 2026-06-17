"""Domain operations for papers — pure orchestration over the repositories.

This layer holds no SQL, no ORM types, and no Rich/CLI concerns. It depends only
on the repository classes and DTOs exposed by :mod:`paper_sorts.db.repositories`.
``update_field`` dispatches over a ``Literal`` table argument with
``assert_never`` for compile-time exhaustiveness.
"""

from __future__ import annotations

from typing import Literal, assert_never

from sqlalchemy import Engine

from paper_sorts.db.repositories import (
    PaperCreate,
    PaperRepository,
    PaperSummary,
)
from paper_sorts.db.session import with_session

UpdatableTable = Literal["papers", "bib", "authors_id"]


class PaperService:
    """High-level paper operations, each wrapped in a managed transaction."""

    def __init__(self, engine: Engine) -> None:
        """Bind the service to an engine.

        :param engine: the engine used to open per-operation sessions.
        """
        self.engine = engine

    def search_by_title(self, title: str) -> list[PaperSummary]:
        """Return paper summaries whose title exactly matches ``title``.

        :param title: the title to search for.
        :returns: matching summaries (empty if none).
        """
        with with_session(self.engine) as session:
            return PaperRepository(session).search_by_title(title)

    def search_by_author(self, author: str) -> list[PaperSummary]:
        """Return paper summaries credited to ``author``.

        :param author: the ``"Last, First"`` author name.
        :returns: matching summaries (empty if none).
        """
        with with_session(self.engine) as session:
            return PaperRepository(session).search_by_author(author)

    def add_paper(self, data: PaperCreate) -> int:
        """Add a paper (with bib and authors) in one transaction.

        :param data: the paper to insert.
        :returns: the new paper id.
        :raises DuplicateError: if the BibTeX key already exists.
        """
        with with_session(self.engine) as session:
            return PaperRepository(session).add_paper(data)

    def delete_paper(self, bibtex_id: str) -> None:
        """Delete a paper and its dependent rows by BibTeX key.

        :param bibtex_id: the BibTeX key of the paper to delete.
        :raises NotFoundError: if the paper does not exist.
        """
        with with_session(self.engine) as session:
            PaperRepository(session).delete_paper(bibtex_id)

    def update_field(
        self,
        table: UpdatableTable,
        column: str,
        identifier: str,
        value: str,
    ) -> None:
        """Update a single editable field, rejecting non-editable targets.

        :param table: one of ``"papers"``, ``"bib"``, ``"authors_id"``.
        :param column: the column to update (validated per table).
        :param identifier: the row key (paper id, bibtex key, or author name).
        :param value: the new value.
        :raises ValueError: for ``*_id`` columns, the ``authors_papers`` table,
            unknown tables/columns, duplicates, or missing rows.
        """
        if column.endswith("_id"):
            raise ValueError("IDs are unique and must not be changed!")
        with with_session(self.engine) as session:
            repo = PaperRepository(session)
            match table:
                case "papers":
                    repo.update_paper_field(int(identifier), column, value)
                case "bib":
                    if column != "bibtex":
                        raise ValueError(
                            f"Column {column!r} is not present in table bib"
                        )
                    repo.bib.update_bibtex(identifier, value)
                case "authors_id":
                    if column != "author":
                        raise ValueError(
                            f"Column {column!r} is not present in table authors_id"
                        )
                    repo.authors.rename_author(identifier, value)
                case _:  # pragma: no cover - exhaustiveness guard
                    assert_never(table)


def reject_authors_papers_update() -> None:
    """Raise the legacy error for attempts to update the link table.

    The ``authors_papers`` table is not user-editable; this preserves the
    legacy ``ValueError`` for that path.

    :raises ValueError: always.
    """
    raise ValueError("Table authors_papers has no changeable column!")
