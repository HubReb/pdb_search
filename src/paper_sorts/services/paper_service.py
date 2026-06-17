"""High-level domain operations for papers.

Pure orchestration over the persistence layer: this module opens sessions and
delegates to the repositories, but contains no SQL, no ``rich``, and no direct
I/O. It depends on DTOs (:class:`PaperSummary`, :class:`PaperCreate`) and the
repositories, never on SQLAlchemy ORM types.
"""

from __future__ import annotations

from typing import Literal, assert_never

from sqlalchemy import Engine

from paper_sorts.db.repositories import (
    AuthorRepository,
    BibRepository,
    PaperCreate,
    PaperNotFoundError,
    PaperRepository,
    PaperSummary,
)
from paper_sorts.db.session import with_session

#: The tables whose rows can be targeted by :func:`update_field`.
UpdatableTable = Literal["papers", "bib", "authors_id", "authors_papers"]


class UnknownColumnError(Exception):
    """Raised when a column is not editable for the requested table."""


def search_by_title(engine: Engine, title: str) -> list[PaperSummary]:
    """Return every paper whose title matches exactly.

    :param engine: the database engine.
    :param title: exact title to look up.
    :return: matching paper summaries (possibly several sharing a title).
    """
    with with_session(engine) as session:
        return PaperRepository(session).search_by_title(title)


def search_by_author(engine: Engine, author: str) -> list[PaperSummary]:
    """Return every paper credited to the given author.

    :param engine: the database engine.
    :param author: exact author name (``"Last, First"``).
    :return: matching paper summaries.
    """
    with with_session(engine) as session:
        return PaperRepository(session).search_by_author(author)


def add_paper(engine: Engine, paper: PaperCreate) -> None:
    """Persist a paper atomically (bib row, paper row, author links).

    :param engine: the database engine.
    :param paper: the paper to add.
    :raises DuplicateBibtexError: if the BibTeX key or source already exists.
    """
    with with_session(engine) as session:
        PaperRepository(session).add(paper)


def update_field(
    engine: Engine,
    table: UpdatableTable,
    column: str,
    value: str,
    identifier: str,
) -> None:
    """Update one editable column of one row, mirroring legacy ``update_entry``.

    ID columns are never editable, and ``authors_papers`` has no editable column.
    Dispatch is a ``match``/``case`` over the ``table`` literal with
    :func:`assert_never` for compile-time exhaustiveness.

    :param engine: the database engine.
    :param table: the table to update (one of the canonical four).
    :param column: the column to update.
    :param value: the new value.
    :param identifier: the row identifier (a ``papers.id`` / ``authors_id.id``
        integer as text, or a ``bib.bibtex_id`` key).
    :raises UnknownColumnError: if the column is not editable for the table.
    :raises PaperNotFoundError: if the targeted row does not exist.
    """
    if "_id" in column or column == "id":
        raise UnknownColumnError(f"Column {column!r} is an identifier and cannot be edited")

    with with_session(engine) as session:
        match table:
            case "papers":
                repo = PaperRepository(session)
                paper_id = int(identifier)
                if column == "title":
                    repo.update_title(paper_id, value)
                elif column == "contents":
                    repo.update_contents(paper_id, value)
                else:
                    raise UnknownColumnError(
                        f"Column {column!r} is not editable on table 'papers'"
                    )
            case "bib":
                if column != "bibtex":
                    raise UnknownColumnError(
                        f"Column {column!r} is not editable on table 'bib'"
                    )
                BibRepository(session).update_bibtex(identifier, value)
            case "authors_id":
                if column != "author":
                    raise UnknownColumnError(
                        f"Column {column!r} is not editable on table 'authors_id'"
                    )
                AuthorRepository(session).rename(int(identifier), value)
            case "authors_papers":
                raise UnknownColumnError("Table 'authors_papers' has no editable column")
            case _:  # pragma: no cover - exhaustiveness guard
                assert_never(table)


def delete_paper(engine: Engine, paper_id: int) -> None:
    """Delete a paper, its author links, orphaned authors, and bib row.

    :param engine: the database engine.
    :param paper_id: internal paper id.
    :raises PaperNotFoundError: if the paper does not exist.
    """
    with with_session(engine) as session:
        PaperRepository(session).delete(paper_id)


__all__ = [
    "PaperNotFoundError",
    "UnknownColumnError",
    "UpdatableTable",
    "add_paper",
    "delete_paper",
    "search_by_author",
    "search_by_title",
    "update_field",
]
