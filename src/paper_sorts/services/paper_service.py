"""Domain service for paper operations.

Provides high-level operations over the paper database:
- :func:`search_by_title`
- :func:`search_by_author`
- :func:`add_paper`
- :func:`update_field`
- :func:`delete_paper`

This module has **no** SQLAlchemy imports.  It interacts with the database
exclusively through the repository classes in :mod:`paper_sorts.db.repositories`
and the session factory passed as ``with_session_fn`` (constitution Principle I).

Usage::

    from paper_sorts.services.paper_service import search_by_title
    from paper_sorts.db.session import with_session

    results = search_by_title("some title", with_session_fn=with_session)
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Any, Literal

from paper_sorts.db.repositories import PaperCreate, PaperRepository, PaperSummary

logger = logging.getLogger(__name__)

# Type alias for the session factory callable received by all service functions.
SessionFactory = Callable[[str], AbstractContextManager[Any]]

# Updatable fields — used in update_field's Literal type and match/case.
UpdatableField = Literal["title", "contents", "bibtex", "author"]


def search_by_title(
    title: str,
    *,
    database_url: str,
    with_session_fn: SessionFactory,
) -> list[PaperSummary]:
    """Search for papers by exact title match.

    :param title: Exact title string to search for (case-sensitive).
    :param database_url: PostgreSQL DSN.
    :param with_session_fn: Session factory; default is
        :func:`paper_sorts.db.session.with_session`.
    :returns: List of :class:`PaperSummary` objects (may be empty).
    """
    logger.debug("search_by_title: %r", title)
    with with_session_fn(database_url) as session:
        return PaperRepository.search_by_title(session, title)


def search_by_author(
    author: str,
    *,
    database_url: str,
    with_session_fn: SessionFactory,
) -> list[PaperSummary]:
    """Search for papers by exact author name match.

    :param author: Author name in ``"Last, First"`` form.
    :param database_url: PostgreSQL DSN.
    :param with_session_fn: Session factory.
    :returns: List of :class:`PaperSummary` objects (may be empty).
    """
    logger.debug("search_by_author: %r", author)
    with with_session_fn(database_url) as session:
        return PaperRepository.search_by_author(session, author)


def add_paper(
    paper: PaperCreate,
    *,
    database_url: str,
    with_session_fn: SessionFactory,
) -> PaperSummary:
    """Insert a new paper into the database.

    :param paper: :class:`PaperCreate` DTO with all required fields.
    :param database_url: PostgreSQL DSN.
    :param with_session_fn: Session factory.
    :returns: :class:`PaperSummary` for the newly created paper.
    :raises Exception: Re-raises any database error after rolling back.
    """
    logger.debug("add_paper: bibtex_key=%r", paper.bibtex_key)
    with with_session_fn(database_url) as session:
        return PaperRepository.create(session, paper)


def update_field(
    paper_id: int,
    field: UpdatableField,
    value: str,
    *,
    database_url: str,
    with_session_fn: SessionFactory,
) -> None:
    """Update a single field on an existing paper.

    Uses ``match``/``case`` over the ``UpdatableField`` literal for
    compile-time exhaustiveness (``assert_never`` at the end).

    Supported fields:
    - ``"title"`` — updates the paper title
    - ``"contents"`` — updates the paper summary
    - ``"bibtex"`` — updates the BibTeX source string
    - ``"author"`` — replaces all authors with a single new author name

    :param paper_id: ``papers.id`` of the paper to update.
    :param field: Field name to update.
    :param value: New value for the field.
    :param database_url: PostgreSQL DSN.
    :param with_session_fn: Session factory.
    :raises ValueError: If *paper_id* is not found.
    """
    from typing import assert_never

    logger.debug("update_field: paper_id=%d field=%r", paper_id, field)

    # Validate field at call-site via match/case; assert_never guards against
    # future additions to UpdatableField that are not handled here.
    match field:
        case "title":
            repo_field = "title"
        case "contents":
            repo_field = "contents"
        case "bibtex":
            repo_field = "bibtex"
        case "author":
            repo_field = "author"
        case _:
            assert_never(field)

    with with_session_fn(database_url) as session:
        PaperRepository.update_field(session, paper_id, repo_field, value)


def delete_paper(
    paper_id: int,
    *,
    database_url: str,
    with_session_fn: SessionFactory,
) -> None:
    """Delete a paper and its author links from the database.

    :param paper_id: ``papers.id`` of the paper to delete.
    :param database_url: PostgreSQL DSN.
    :param with_session_fn: Session factory.
    :raises ValueError: If *paper_id* is not found.
    """
    logger.debug("delete_paper: paper_id=%d", paper_id)
    with with_session_fn(database_url) as session:
        PaperRepository.delete(session, paper_id)
