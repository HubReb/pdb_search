"""Domain orchestration for paper operations.

The service layer is pure orchestration over DTOs and repositories — no SQL, no
Rich, no I/O (Constitution Principle I, FR-014). Each operation opens a single
:func:`~paper_sorts.db.session.with_session` transaction so that a failure rolls
back atomically (this subsumes the legacy hand-coded ``rollback_database_addition``).
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
    """High-level paper operations backed by repositories over a real DB."""

    def __init__(self, engine: Engine) -> None:
        """:param engine: the database engine to open sessions against."""
        self._engine = engine

    def search_by_title(self, title: str) -> list[PaperSummary]:
        """Return papers whose title matches exactly.

        :param title: the title to search for.
        :return: matching paper summaries (possibly several with the same title).
        """
        with with_session(self._engine) as session:
            return PaperRepository(session).search_by_title(title)

    def search_by_author(self, author: str) -> list[PaperSummary]:
        """Return papers an author is credited on.

        :param author: the author name to search for.
        :return: the author's paper summaries.
        """
        with with_session(self._engine) as session:
            return PaperRepository(session).search_by_author(author)

    def add_paper(self, paper: PaperCreate) -> None:
        """Add a paper and all its related rows in one transaction.

        :param paper: the paper to add.
        :raises DuplicateBibtexKeyError: if the BibTeX key already exists.
        """
        with with_session(self._engine) as session:
            PaperRepository(session).add(paper)

    def update_field(
        self,
        table: UpdatableTable,
        column: str,
        identifier: str,
        new_value: str,
    ) -> None:
        """Update one editable field, dispatching on the target table.

        IDs and the BibTeX key are immutable. ``authors_papers`` is not an
        updatable target. Exhaustiveness over ``table`` is checked at type-time
        via :func:`assert_never`.

        :param table: one of ``"papers"``, ``"bib"``, ``"authors_id"``.
        :param column: the column to update (e.g. ``"title"``, ``"contents"``,
            ``"bibtex"``, ``"author"``).
        :param identifier: the row identifier (paper id, bibtex_id, or author name).
        :param new_value: the new value.
        :raises ValueError: if the column is not editable for the table.
        :raises PaperNotFoundError: if the target row is absent.
        :raises DuplicateBibtexKeyError: if a new BibTeX collides on uniqueness.
        """
        with with_session(self._engine) as session:
            repo = PaperRepository(session)
            match table:
                case "papers":
                    if column == "title":
                        repo.update_title(int(identifier), new_value)
                    elif column == "contents":
                        repo.update_contents(int(identifier), new_value)
                    else:
                        raise ValueError(f"column {column!r} is not editable in papers")
                case "bib":
                    if column != "bibtex":
                        raise ValueError(f"column {column!r} is not editable in bib")
                    repo._bib.update_bibtex(identifier, new_value)
                case "authors_id":
                    if column != "author":
                        raise ValueError(f"column {column!r} is not editable in authors_id")
                    repo._authors.rename(identifier, new_value)
                case _:
                    assert_never(table)

    def delete_paper(self, summary: PaperSummary) -> None:
        """Delete a paper and its dependent rows in one transaction.

        :param summary: the paper to delete.
        :raises PaperNotFoundError: if the paper does not exist.
        """
        with with_session(self._engine) as session:
            PaperRepository(session).delete(summary)
