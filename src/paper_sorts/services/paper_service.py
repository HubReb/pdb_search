"""Domain-level paper operations for paper_sorts.

This module contains pure orchestration functions: they open one session per
call (via :func:`~paper_sorts.db.session.with_session`), delegate to the
repository, and return plain DTOs.  No SQL, no rich, no I/O live here.

:data:`_repo` is a module-level singleton repository instance (no state,
so sharing is safe).
"""

from __future__ import annotations

import logging
from typing import Literal

from sqlalchemy.engine import Engine

from paper_sorts.db.repositories import PaperCreate, PaperRepository, PaperSummary
from paper_sorts.db.session import with_session

logger = logging.getLogger(__name__)

_repo = PaperRepository()


def search_by_title(engine: Engine, title: str) -> list[PaperSummary]:
    """Search for papers by exact title match.

    :param engine: SQLAlchemy engine (created by :func:`~paper_sorts.db.session.get_engine`).
    :param title: Title string to search for (case-sensitive equality).
    :return: List of :class:`~paper_sorts.db.repositories.PaperSummary` DTOs.
    """
    with with_session(engine) as session:
        return _repo.search_by_title(session, title)


def search_by_author(engine: Engine, author: str) -> list[PaperSummary]:
    """Search for papers by author name.

    :param engine: SQLAlchemy engine.
    :param author: Author name string (case-sensitive equality).
    :return: List of :class:`~paper_sorts.db.repositories.PaperSummary` DTOs.
    """
    with with_session(engine) as session:
        return _repo.search_by_author(session, author)


def add_paper(engine: Engine, paper: PaperCreate) -> PaperSummary:
    """Add a new paper to the database.

    :param engine: SQLAlchemy engine.
    :param paper: :class:`~paper_sorts.db.repositories.PaperCreate` DTO.
    :return: :class:`~paper_sorts.db.repositories.PaperSummary` of the new paper.
    :raises sqlalchemy.exc.IntegrityError: If bibtex_id or bibtex already exist.
    """
    with with_session(engine) as session:
        return _repo.add_paper(session, paper)


def update_field(
    engine: Engine,
    paper_id: int,
    table: Literal["papers", "bib", "authors_id"],
    field: str,
    value: str,
) -> None:
    """Update a single field of a paper-related row.

    :param engine: SQLAlchemy engine.
    :param paper_id: ID from ``papers.id`` (or author_id when updating ``authors_id``).
    :param table: Target table — ``"papers"``, ``"bib"``, or ``"authors_id"``.
    :param field: Column name to update.
    :param value: New value.
    :raises ValueError: If the table/field combination is not supported.
    :raises ValueError: If a unique constraint would be violated.
    """
    with with_session(engine) as session:
        _repo.update_field(session, paper_id, table, field, value)


def delete_paper(engine: Engine, paper_id: int) -> None:
    """Delete a paper and its associated author links and bib entry.

    :param engine: SQLAlchemy engine.
    :param paper_id: ID from ``papers.id``.
    :raises ValueError: If no paper with that ID exists.
    """
    with with_session(engine) as session:
        _repo.delete_paper(session, paper_id)
