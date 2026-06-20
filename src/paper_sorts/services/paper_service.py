"""Domain-layer service for paper_sorts.

Contains high-level operations that coordinate between repositories.
This module has **no** SQLAlchemy imports — it depends only on the
repository classes and the DTOs from :mod:`paper_sorts.db.repositories`.
The SQLAlchemy engine is accepted as a parameter so callers (CLI commands)
control the session lifecycle.

Operations
----------
- :func:`search_by_title` — find papers by exact title
- :func:`search_by_author` — find papers by author name
- :func:`add_paper` — insert a new paper with all related rows
- :func:`update_field` — update a single field in papers / bib / authors_id
- :func:`delete_paper` — remove a paper and all associated rows
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Literal, assert_never

from paper_sorts.db.repositories import (
    AuthorRepository,
    BibRepository,
    PaperCreate,
    PaperRepository,
    PaperSummary,
)
from paper_sorts.db.session import with_session

if TYPE_CHECKING:
    from sqlalchemy import Engine
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Allowed table names for update_field
TableName = Literal["papers", "bib", "authors_id"]


def search_by_title(engine: Engine, title: str) -> list[PaperSummary]:
    """Return all papers whose title exactly matches *title*.

    :param engine: SQLAlchemy engine connected to the target database.
    :param title: Exact title string to search for.
    :returns: List of :class:`~paper_sorts.db.repositories.PaperSummary`
        objects (empty list if not found).
    """
    with with_session(engine) as session:
        return PaperRepository.search_by_title(session, title)


def search_by_author(engine: Engine, author: str) -> list[PaperSummary]:
    """Return all papers attributed to *author*.

    :param engine: SQLAlchemy engine connected to the target database.
    :param author: Author name in ``"Last, First"`` form.
    :returns: List of :class:`~paper_sorts.db.repositories.PaperSummary`
        objects (empty list if not found).
    """
    with with_session(engine) as session:
        return PaperRepository.search_by_author(session, author)


def add_paper(engine: Engine, paper: PaperCreate) -> None:
    """Persist a new paper (bib entry, paper row, author rows, link rows).

    :param engine: SQLAlchemy engine connected to the target database.
    :param paper: :class:`~paper_sorts.db.repositories.PaperCreate` DTO.
    :raises ValueError: If the BibTeX key already exists in the database.
    :raises ValueError: If *paper.authors* is empty.
    """
    with with_session(engine) as session:
        PaperRepository.add_paper(session, paper)
    logger.info("Added paper %r to database", paper.bibtex_id)


def update_field(
    engine: Engine,
    table: TableName,
    identifier: str,
    field: str,
    value: str,
) -> None:
    """Update a single field in *table* for the row identified by *identifier*.

    :param engine: SQLAlchemy engine connected to the target database.
    :param table: Table to update — one of ``"papers"``, ``"bib"``,
        ``"authors_id"``.
    :param identifier: The row identifier:
        - ``"papers"``: numeric ``papers.id`` (as a string).
        - ``"bib"``: the ``bibtex_id`` string.
        - ``"authors_id"``: the author name string (current name).
    :param field: Column/field name to update.
    :param value: New value to set.
    :raises ValueError: If the table/field combination is not supported.
    :raises ValueError: If the row does not exist.
    """
    with with_session(engine) as session:
        match table:
            case "papers":
                _update_papers(session, identifier, field, value)
            case "bib":
                _update_bib(session, identifier, field, value)
            case "authors_id":
                _update_authors_id(session, identifier, field, value)
            case _ as unreachable:
                assert_never(unreachable)
    logger.info("Updated %s.%s for %r to %r", table, field, identifier, value)


def _update_papers(session: Session, identifier: str, field: str, value: str) -> None:
    """Update the ``papers`` table.

    :param session: Active SQLAlchemy session.
    :param identifier: ``papers.id`` as a string.
    :param field: Field to update (``"title"`` or ``"contents"``).
    :param value: New value.
    :raises ValueError: If *field* is not updatable.
    :raises ValueError: If no paper with *identifier* exists.
    """
    from sqlalchemy import update

    from paper_sorts.db.models import Paper

    if field not in ("title", "contents"):
        raise ValueError(f"Cannot update field {field!r} in table 'papers'")

    try:
        paper_id = int(identifier)
    except ValueError as exc:
        raise ValueError(f"papers identifier must be a numeric id, got {identifier!r}") from exc

    from sqlalchemy.engine import CursorResult

    result: CursorResult[Any] = session.execute(  # type: ignore[assignment]
        update(Paper).where(Paper.id == paper_id).values(**{field: value})
    )
    if result.rowcount == 0:
        raise ValueError(f"Paper with id={paper_id} not found")


def _update_bib(session: Session, identifier: str, field: str, value: str) -> None:
    """Update the ``bib`` table.

    :param session: Active SQLAlchemy session.
    :param identifier: ``bibtex_id`` of the bib entry.
    :param field: Field to update (only ``"bibtex"`` is permitted).
    :param value: New BibTeX string.
    :raises ValueError: If *field* is not ``"bibtex"``.
    """
    match field:
        case "bibtex":
            BibRepository.update_bibtex(session, identifier, value)
        case _:
            raise ValueError(f"Cannot update field {field!r} in table 'bib'")


def _update_authors_id(session: Session, identifier: str, field: str, value: str) -> None:
    """Update the ``authors_id`` table.

    :param session: Active SQLAlchemy session.
    :param identifier: Current author name (used as the lookup key).
    :param field: Field to update (only ``"author"`` is permitted).
    :param value: New author name.
    :raises ValueError: If *field* is not ``"author"``.
    """
    match field:
        case "author":
            AuthorRepository.update_author_name(session, identifier, value)
        case _:
            raise ValueError(f"Cannot update field {field!r} in table 'authors_id'")


def delete_paper(engine: Engine, bibtex_id: str) -> None:
    """Delete a paper and all associated rows identified by *bibtex_id*.

    :param engine: SQLAlchemy engine connected to the target database.
    :param bibtex_id: BibTeX citation key of the paper to remove.
    :raises ValueError: If no paper with *bibtex_id* exists.
    """
    with with_session(engine) as session:
        PaperRepository.delete_paper(session, bibtex_id)
    logger.info("Deleted paper %r from database", bibtex_id)
