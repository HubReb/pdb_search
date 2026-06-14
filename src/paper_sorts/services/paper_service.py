"""Domain service layer for paper_sorts.

Pure orchestration: no SQL, no rich, no I/O. All database interaction
goes through the repository classes in paper_sorts.db.repositories.
Services depend only on DTOs (PaperCreate, PaperSummary), never on ORM types.
"""

from __future__ import annotations

import logging
from typing import Literal

from paper_sorts.db.repositories import (
    AuthorRepository,
    BibRepository,
    PaperCreate,
    PaperRepository,
    PaperSummary,
)
from paper_sorts.db.session import with_session

logger = logging.getLogger(__name__)

# Literal type for the table/field argument of update_field
UpdateTarget = Literal["title", "contents", "bibtex", "author"]


def search_by_title(database_url: str, title: str) -> list[PaperSummary]:
    """Search for papers by title.

    :param database_url: PostgreSQL DSN
    :param title: search term (case-insensitive substring match)
    :return: list of matching PaperSummary DTOs
    """
    with with_session(database_url) as session:
        repo = PaperRepository(session)
        results = repo.search_by_title(title)
    logger.info("search_by_title('%s') returned %d result(s)", title, len(results))
    return results


def search_by_author(database_url: str, author_name: str) -> list[PaperSummary]:
    """Search for papers by author name.

    :param database_url: PostgreSQL DSN
    :param author_name: author name fragment (case-insensitive substring match)
    :return: list of matching PaperSummary DTOs
    :raises KeyError: if no matching author is found
    """
    with with_session(database_url) as session:
        repo = PaperRepository(session)
        results = repo.search_by_author(author_name)
    logger.info("search_by_author('%s') returned %d result(s)", author_name, len(results))
    return results


def add_paper(database_url: str, data: PaperCreate) -> PaperSummary:
    """Add a new paper to the database.

    :param database_url: PostgreSQL DSN
    :param data: PaperCreate DTO with title, contents, bibtex_id, bibtex, authors
    :return: PaperSummary DTO for the newly created paper
    :raises ValueError: if a paper with the same bibtex_id already exists
    """
    with with_session(database_url) as session:
        repo = PaperRepository(session)
        result = repo.create(data)
    logger.info("add_paper: inserted '%s' (%s)", data.title, data.bibtex_id)
    return result


def update_field(
    database_url: str,
    bibtex_id: str,
    field: UpdateTarget,
    new_value: str,
) -> PaperSummary:
    """Update a single field of an existing paper.

    Uses match/case over the UpdateTarget Literal to ensure compile-time
    exhaustiveness (assert_never for unknown values).

    :param database_url: PostgreSQL DSN
    :param bibtex_id: BibTeX citation key identifying the paper to update
    :param field: which field to update: 'title', 'contents', 'bibtex', or 'author'
    :param new_value: new value for the field
    :return: updated PaperSummary DTO
    :raises KeyError: if the paper (or author for 'author' field) is not found
    :raises ValueError: if the new bibtex value is already taken (UNIQUE constraint)
    :raises AssertionError: if field is not one of the known UpdateTarget values
    """
    from typing import assert_never

    with with_session(database_url) as session:
        paper_repo = PaperRepository(session)
        match field:
            case "title":
                result = paper_repo.update_title(bibtex_id, new_value)
            case "contents":
                result = paper_repo.update_contents(bibtex_id, new_value)
            case "bibtex":
                bib_repo = BibRepository(session)
                bib_repo.update(bibtex_id, new_value)
                result_opt = paper_repo.get_by_bibtex_id(bibtex_id)
                if result_opt is None:
                    raise KeyError(f"Paper '{bibtex_id}' not found after bibtex update.")
                result = result_opt
            case "author":
                # new_value is expected as "OldName -> NewName"
                if " -> " not in new_value:
                    raise ValueError(
                        "For author updates, provide 'Old Name -> New Name'."
                    )
                old_name, new_name = new_value.split(" -> ", 1)
                author_repo = AuthorRepository(session)
                author_repo.update_name(old_name.strip(), new_name.strip())
                result_opt = paper_repo.get_by_bibtex_id(bibtex_id)
                if result_opt is None:
                    raise KeyError(f"Paper '{bibtex_id}' not found.")
                result = result_opt
            case other:
                assert_never(other)
    logger.info("update_field: %s.%s for '%s'", bibtex_id, field, bibtex_id)
    return result


def delete_paper(database_url: str, bibtex_id: str) -> str:
    """Delete a paper and its associated data.

    Removes the paper, its BibTeX entry, its authorship links, and any
    authors that have no remaining papers after the delete.

    :param database_url: PostgreSQL DSN
    :param bibtex_id: BibTeX citation key
    :return: title of the deleted paper
    :raises KeyError: if no paper found for the given bibtex_id
    """
    with with_session(database_url) as session:
        repo = PaperRepository(session)
        title = repo.delete(bibtex_id)
    logger.info("delete_paper: deleted '%s' (%s)", title, bibtex_id)
    return title
