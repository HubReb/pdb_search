"""Domain operations for paper_sorts.

Pure orchestration layer — no SQL, no I/O, no rich.  Calls repository methods
and returns DTOs.  The update_field function uses match/case with assert_never
for compile-time exhaustiveness (Principle I).
"""

from __future__ import annotations

import logging
from typing import Literal, assert_never

from sqlalchemy.orm import Session

from paper_sorts.db.repositories import PaperCreate, PaperRepository, PaperSummary

logger = logging.getLogger(__name__)

# Allowed field names for update_field
UpdateableField = Literal["title", "contents", "bibtex", "authors"]


def search_by_title(session: Session, title: str) -> list[PaperSummary]:
    """Search for papers whose title contains the given substring.

    Args:
        session: An open SQLAlchemy Session.
        title: Title search string (partial, case-insensitive match).

    Returns:
        List of matching PaperSummary DTOs, possibly empty.
    """
    repo = PaperRepository(session)
    results = repo.search_by_title(title)
    logger.debug("search_by_title(%r) → %d results", title, len(results))
    return results


def search_by_author(session: Session, author: str) -> list[PaperSummary]:
    """Search for papers with an author whose name contains the search string.

    Args:
        session: An open SQLAlchemy Session.
        author: Author name search string (partial, case-insensitive match).

    Returns:
        List of matching PaperSummary DTOs, possibly empty.
    """
    repo = PaperRepository(session)
    results = repo.search_by_author(author)
    logger.debug("search_by_author(%r) → %d results", author, len(results))
    return results


def add_paper(session: Session, paper: PaperCreate) -> PaperSummary:
    """Add a new paper to the database.

    Args:
        session: An open SQLAlchemy Session.
        paper: PaperCreate DTO with all required fields non-empty.

    Returns:
        PaperSummary DTO for the newly created paper.

    Raises:
        ValueError: If required fields are empty.
        sqlalchemy.exc.IntegrityError: If bibtex_id or bibtex already exists.
    """
    if not paper.title:
        raise ValueError("title must not be empty")
    if not paper.bibtex_id:
        raise ValueError("bibtex_id must not be empty")
    if not paper.authors:
        raise ValueError("at least one author is required")
    repo = PaperRepository(session)
    result = repo.add(paper)
    logger.info("added paper bibtex_id=%r id=%d", result.bibtex_id, result.id)
    return result


def update_field(
    session: Session,
    paper_id: int,
    field: UpdateableField,
    value: str | list[str],
) -> PaperSummary:
    """Update a single field of an existing paper.

    Uses match/case with assert_never for compile-time exhaustiveness over
    the UpdateableField Literal type.

    Args:
        session: An open SQLAlchemy Session.
        paper_id: The papers.id to update.
        field: Which field to update: 'title', 'contents', 'bibtex', or 'authors'.
        value: New value; str for title/contents/bibtex, list[str] for authors.

    Returns:
        Updated PaperSummary DTO.

    Raises:
        ValueError: If paper_id does not exist.
        TypeError: If value type does not match the field.
    """
    repo = PaperRepository(session)
    match field:
        case "title":
            if not isinstance(value, str):
                raise TypeError("value must be str for field 'title'")
            result = repo.update_title(paper_id, value)
        case "contents":
            if not isinstance(value, str):
                raise TypeError("value must be str for field 'contents'")
            result = repo.update_contents(paper_id, value)
        case "bibtex":
            if not isinstance(value, str):
                raise TypeError("value must be str for field 'bibtex'")
            result = repo.update_bibtex(paper_id, value)
        case "authors":
            if not isinstance(value, list):
                raise TypeError("value must be list[str] for field 'authors'")
            result = repo.update_authors(paper_id, value)
        case _ as unreachable:
            assert_never(unreachable)
    logger.info("updated paper id=%d field=%r", paper_id, field)
    return result


def delete_paper(session: Session, paper_id: int) -> None:
    """Delete a paper and its authorship links.

    Args:
        session: An open SQLAlchemy Session.
        paper_id: The papers.id to delete.

    Raises:
        ValueError: If paper_id does not exist.
    """
    repo = PaperRepository(session)
    repo.delete(paper_id)
    logger.info("deleted paper id=%d", paper_id)
