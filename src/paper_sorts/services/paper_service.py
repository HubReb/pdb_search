"""Domain service layer for paper_sorts.

Pure orchestration: no SQL, no rich, no I/O. Depends on repository DTOs only —
never on ORM types. All SQL is delegated to the persistence layer.

The update_field method uses match/case with assert_never for compile-time
exhaustiveness over the Literal table type.
"""

from __future__ import annotations

import logging
from typing import Literal, assert_never

from sqlalchemy.orm import Session

from paper_sorts.db.repositories import (
    AuthorRepository,
    BibRepository,
    PaperCreate,
    PaperRepository,
    PaperSummary,
)

logger = logging.getLogger(__name__)

# Valid table names accepted by update_field
TableName = Literal["papers", "bib", "authors_id"]


def search_by_title(session: Session, title: str) -> list[PaperSummary]:
    """Search for papers by exact title match.

    :param session: Active SQLAlchemy session.
    :param title: Exact paper title to search for.
    :return: List of matching PaperSummary DTOs (may be empty).
    """
    repo = PaperRepository(session)
    results = repo.search_by_title(title)
    if not results:
        logger.info("No paper found with title '%s'.", title)
    return results


def search_by_author(session: Session, author: str) -> list[PaperSummary]:
    """Search for papers by author name.

    :param session: Active SQLAlchemy session.
    :param author: Author name in 'Last, First' form.
    :return: List of matching PaperSummary DTOs (may be empty).
    """
    repo = PaperRepository(session)
    results = repo.search_by_author(author)
    if not results:
        logger.info("No paper found for author '%s'.", author)
    return results


def add_paper(session: Session, paper: PaperCreate) -> bool:
    """Add a new paper with its BibTeX entry and authors.

    :param session: Active SQLAlchemy session.
    :param paper: PaperCreate DTO with all required fields.
    :return: True if the paper was added successfully, False if already exists.
    """
    repo = PaperRepository(session)
    try:
        repo.add(paper)
        logger.info("Added paper '%s' (%s).", paper.title, paper.bibtex_id)
        return True
    except ValueError as exc:
        logger.warning("Could not add paper '%s': %s", paper.bibtex_id, exc)
        return False


def update_field(
    session: Session,
    table: TableName,
    column: str,
    identifier: str,
    value: str,
) -> None:
    """Update a single field in the specified table.

    Uses match/case with assert_never for compile-time exhaustiveness.

    :param session: Active SQLAlchemy session.
    :param table: Table to update ('papers', 'bib', or 'authors_id').
    :param column: Column name within the table.
    :param identifier: Row identifier (title for papers, bibtex_id for bib,
        author name for authors_id).
    :param value: New value to set.
    :raises ValueError: If the table/column combination is invalid or the row
        is not found.
    """
    match table:
        case "papers":
            paper_repo = PaperRepository(session)
            match column:
                case "title":
                    # Find by current title, update it
                    results = paper_repo.search_by_title(identifier)
                    if not results:
                        raise ValueError(f"No paper found with title '{identifier}'.")
                    paper_repo.update_title(results[0].paper_id, value)
                    logger.info("Updated title of paper %d to '%s'.", results[0].paper_id, value)
                case "contents":
                    results = paper_repo.search_by_title(identifier)
                    if not results:
                        raise ValueError(f"No paper found with title '{identifier}'.")
                    paper_repo.update_contents(results[0].paper_id, value)
                    logger.info("Updated contents of paper %d.", results[0].paper_id)
                case _:
                    raise ValueError(
                        f"Column '{column}' cannot be updated in the 'papers' table. "
                        "Valid columns: title, contents."
                    )
        case "bib":
            bib_repo = BibRepository(session)
            match column:
                case "bibtex":
                    bib_repo.update(identifier, value)
                    logger.info("Updated bibtex for '%s'.", identifier)
                case _:
                    raise ValueError(
                        f"Column '{column}' cannot be updated in the 'bib' table. "
                        "Valid columns: bibtex."
                    )
        case "authors_id":
            author_repo = AuthorRepository(session)
            match column:
                case "author":
                    author_repo.update_name(identifier, value)
                    logger.info("Renamed author '%s' → '%s'.", identifier, value)
                case _:
                    raise ValueError(
                        f"Column '{column}' cannot be updated in the 'authors_id' table. "
                        "Valid columns: author."
                    )
        case _:
            assert_never(table)


def delete_paper(session: Session, bibtex_id: str) -> bool:
    """Delete a paper and its BibTeX entry and author links.

    Author rows in authors_id are NOT deleted (they may be linked to other papers).

    :param session: Active SQLAlchemy session.
    :param bibtex_id: BibTeX key of the paper to delete.
    :return: True if deleted, False if not found.
    """
    repo = PaperRepository(session)
    deleted = repo.delete(bibtex_id)
    if deleted:
        logger.info("Deleted paper '%s'.", bibtex_id)
    else:
        logger.warning("Paper '%s' not found, nothing deleted.", bibtex_id)
    return deleted
