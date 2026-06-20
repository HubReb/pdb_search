"""Domain operations for paper_sorts.

Pure orchestration — no SQL, no rich, no I/O.
Depends on repositories and DTOs only (never on ORM models directly).
"""

import logging
from typing import Literal, assert_never

from sqlalchemy.engine import Engine

from paper_sorts.db.repositories import PaperCreate, PaperRepository, PaperSummary
from paper_sorts.db.session import with_session

logger = logging.getLogger(__name__)

# Tables whose fields are updatable via update_field
UpdateTable = Literal["papers", "bib", "authors_id"]


def search_by_title(engine: Engine, title: str) -> list[PaperSummary]:
    """Return all papers whose title exactly matches *title*.

    :param engine: Active SQLAlchemy engine.
    :param title: Exact title string to search for.
    :returns: List of :class:`~paper_sorts.db.repositories.PaperSummary`
        DTOs; empty if not found.
    """
    with with_session(engine) as session:
        repo = PaperRepository(session)
        return repo.search_by_title(title)


def search_by_author(engine: Engine, author: str) -> list[PaperSummary]:
    """Return all papers by the named author.

    :param engine: Active SQLAlchemy engine.
    :param author: Author name in "Last, First" form.
    :returns: List of :class:`~paper_sorts.db.repositories.PaperSummary`
        DTOs; empty if not found.
    """
    with with_session(engine) as session:
        repo = PaperRepository(session)
        return repo.search_by_author(author)


def add_paper(engine: Engine, paper: PaperCreate) -> None:
    """Insert a new paper (with authors and bib entry) into the database.

    :param engine: Active SQLAlchemy engine.
    :param paper: :class:`~paper_sorts.db.repositories.PaperCreate` DTO.
    :raises ValueError: If the bibtex_id already exists.
    """
    with with_session(engine) as session:
        repo = PaperRepository(session)
        repo.add(paper)
        logger.info("Added paper '%s' (bibtex_id=%s)", paper.title, paper.bibtex_id)


def delete_paper(engine: Engine, paper_id: int) -> None:
    """Delete a paper and all associated data.

    :param engine: Active SQLAlchemy engine.
    :param paper_id: The ``papers.id`` to delete.
    :raises ValueError: If *paper_id* does not exist.
    """
    with with_session(engine) as session:
        repo = PaperRepository(session)
        repo.delete(paper_id)
        logger.info("Deleted paper id=%s", paper_id)


def update_field(
    engine: Engine,
    table: UpdateTable,
    column: str,
    identifier: str,
    value: str,
) -> None:
    """Update a single field in the specified table.

    :param engine: Active SQLAlchemy engine.
    :param table: One of ``"papers"``, ``"bib"``, ``"authors_id"``.
    :param column: Column name to update.
    :param identifier: Row identifier (paper id, bibtex_id, or author id as str).
    :param value: New value for the column.
    :raises ValueError: If the table/column combination is unsupported, or
        the identifier is not found.
    """
    match table:
        case "papers":
            _table_arg: UpdateTable = "papers"
        case "bib":
            _table_arg = "bib"
        case "authors_id":
            _table_arg = "authors_id"
        case _:
            assert_never(table)

    with with_session(engine) as session:
        repo = PaperRepository(session)
        repo.update_field(table, column, identifier, value)
        logger.info(
            "Updated %s.%s (identifier=%s)", table, column, identifier
        )
