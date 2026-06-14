"""Domain operations for paper management.

Pure orchestration layer — no SQL, no rich, no I/O. Depends only on
repository classes (DTOs cross the layer boundary; ORM types do not).
"""

import logging
from typing import Literal

from sqlalchemy.orm import Session

from paper_sorts.db.repositories import PaperCreate, PaperRepository, PaperSummary

logger = logging.getLogger(__name__)

# The exhaustive set of updatable fields.
UpdateField = Literal["title", "contents", "bibtex", "author"]


def search_by_title(session: Session, term: str) -> list[PaperSummary]:
    """Search papers by title substring.

    :param session: Active SQLAlchemy session.
    :param term: Substring to search for (case-insensitive).
    :returns: List of matching :class:`~paper_sorts.db.repositories.PaperSummary` DTOs.
    """
    repo = PaperRepository(session)
    results = repo.search_by_title(term)
    logger.debug("search_by_title(%r) → %d results", term, len(results))
    return results


def search_by_author(session: Session, term: str) -> list[PaperSummary]:
    """Search papers by author name substring.

    :param session: Active SQLAlchemy session.
    :param term: Substring to search for (case-insensitive).
    :returns: List of matching :class:`~paper_sorts.db.repositories.PaperSummary` DTOs.
    """
    repo = PaperRepository(session)
    results = repo.search_by_author(term)
    logger.debug("search_by_author(%r) → %d results", term, len(results))
    return results


def add_paper(session: Session, data: PaperCreate) -> PaperSummary:
    """Add a new paper to the database.

    :param session: Active SQLAlchemy session.
    :param data: :class:`~paper_sorts.db.repositories.PaperCreate` DTO.
    :returns: The created :class:`~paper_sorts.db.repositories.PaperSummary` DTO.
    :raises ValueError: if a paper with this bibtex_id already exists.
    """
    repo = PaperRepository(session)
    result = repo.add(data)
    logger.info("Added paper %r (bibtex_id=%r)", data.title, data.bibtex_id)
    return result


def update_field(
    session: Session,
    paper_id: int,
    field: UpdateField,
    value: str,
) -> None:
    """Update a single field on an existing paper.

    Uses match/case with assert_never for compile-time exhaustiveness over
    the UpdateField literal type.

    :param session: Active SQLAlchemy session.
    :param paper_id: Internal paper ID.
    :param field: One of "title", "contents", "bibtex", "author".
    :param value: New value for the field.
    :raises LookupError: if no paper with paper_id exists.
    :raises AssertionError: if field is not an allowed UpdateField value
        (should never happen if callers use the Literal type).
    """
    from typing import assert_never

    repo = PaperRepository(session)
    match field:
        case "title":
            repo.update_title(paper_id, value)
        case "contents":
            repo.update_contents(paper_id, value)
        case "bibtex":
            repo.update_bibtex(paper_id, value)
        case "author":
            repo.update_author(paper_id, value)
        case _:
            assert_never(field)
    logger.info("Updated paper %d field=%r", paper_id, field)


def delete_paper(session: Session, paper_id: int) -> None:
    """Delete a paper and its author links.

    :param session: Active SQLAlchemy session.
    :param paper_id: Internal paper ID.
    :raises LookupError: if no paper with paper_id exists.
    """
    repo = PaperRepository(session)
    repo.delete(paper_id)
    logger.info("Deleted paper %d", paper_id)
