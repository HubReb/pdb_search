"""Domain operations for paper_sorts.

Pure orchestration: no SQL, no I/O, no Rich output.  All database interaction
goes through the repository classes in ``paper_sorts.db.repositories``.

:func:`update_field` uses ``match``/``case`` over a ``Literal[...]`` table
argument with :func:`typing.assert_never` for compile-time exhaustiveness.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Literal

from paper_sorts.db.repositories import (
    AuthorRepository,
    BibRepository,
    PaperCreate,
    PaperRepository,
    PaperSummary,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

log = logging.getLogger(__name__)

# The set of tables that update_field may target.
UpdateTable = Literal["papers", "bib", "authors_id"]


def search_by_title(session: Session, title: str) -> list[PaperSummary]:
    """Search for papers whose title matches *title* exactly.

    :param session: active SQLAlchemy session.
    :param title: exact title string to search for.
    :returns: list of matching :class:`~paper_sorts.db.repositories.PaperSummary`
        objects.  Empty list if nothing found.
    """
    results = PaperRepository.search_by_title(session, title)
    if not results:
        log.info("No paper found with title '%s'.", title)
    return results


def search_by_author(session: Session, author: str) -> list[PaperSummary]:
    """Search for papers attributed to *author*.

    :param session: active SQLAlchemy session.
    :param author: exact author name string in ``"Last, First"`` form.
    :returns: list of matching :class:`~paper_sorts.db.repositories.PaperSummary`
        objects.  Empty list if nothing found.
    """
    results = PaperRepository.search_by_author(session, author)
    if not results:
        log.info("No papers found for author '%s'.", author)
    return results


def add_paper(session: Session, paper: PaperCreate) -> None:
    """Insert a new paper into the database.

    :param session: active SQLAlchemy session.
    :param paper: :class:`~paper_sorts.db.repositories.PaperCreate` DTO with
        all required fields.
    :raises ValueError: if a paper with the same BibTeX key already exists.
    """
    from sqlalchemy.exc import IntegrityError  # local import to keep services/ clean

    try:
        PaperRepository.add_paper(session, paper)
        log.info("Added paper '%s' (%s).", paper.title, paper.bibtex_id)
    except IntegrityError as exc:
        raise ValueError(
            f"A paper with BibTeX key '{paper.bibtex_id}' already exists."
        ) from exc


def update_field(
    session: Session,
    table: UpdateTable,
    identifier: str,
    column: str,
    value: str,
) -> None:
    """Update a single field in *table* identified by *identifier*.

    :param session: active SQLAlchemy session.
    :param table: one of ``"papers"``, ``"bib"``, ``"authors_id"``.
    :param identifier: row identifier — paper_id (str) for ``papers``, bibtex_id
        for ``bib``, or author name for ``authors_id``.
    :param column: column name to update.
    :param value: new value to set.
    :raises ValueError: if *table*, *column*, or *identifier* is invalid.
    """
    match table:
        case "papers":
            try:
                paper_id = int(identifier)
            except ValueError as exc:
                raise ValueError(
                    f"Paper identifier must be an integer ID, got '{identifier}'."
                ) from exc
            PaperRepository.update_paper_field(session, paper_id, column, value)
            log.info("Updated papers.%s for id=%s.", column, identifier)
        case "bib":
            if column != "bibtex":
                raise ValueError(
                    f"Only 'bibtex' is editable in the bib table; got '{column}'."
                )
            BibRepository.update_bib(session, identifier, value)
            log.info("Updated bib.bibtex for bibtex_id='%s'.", identifier)
        case "authors_id":
            if column != "author":
                raise ValueError(
                    f"Only 'author' is editable in the authors_id table; got '{column}'."
                )
            AuthorRepository.update_author_name(session, identifier, value)
            log.info("Renamed author '%s' to '%s'.", identifier, value)
        case _ as unreachable:
            from typing import assert_never

            assert_never(unreachable)


def delete_paper(session: Session, bibtex_id: str) -> None:
    """Delete a paper and all associated records.

    Removes: authorship links, orphaned author rows, the paper row, and the
    BibTeX entry.

    :param session: active SQLAlchemy session.
    :param bibtex_id: BibTeX cite key of the paper to delete.
    :raises ValueError: if no paper with *bibtex_id* exists.
    """
    PaperRepository.delete_paper(session, bibtex_id)
    log.info("Deleted paper with bibtex_id='%s'.", bibtex_id)
