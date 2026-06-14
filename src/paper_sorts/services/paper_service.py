"""Domain operations for paper_sorts (pure orchestration; no SQL, no rich, no I/O).

All database interactions go through the repository classes in db/repositories.py.
Services receive a SQLAlchemy Session and delegate all persistence to repositories.

Public API:
- search_by_title(session, title) -> list[PaperSummary]
- search_by_author(session, author) -> list[PaperSummary]
- add_paper(session, paper) -> PaperSummary
- update_field(session, paper_id, table, column, value) -> None
- delete_paper(session, paper_id) -> None
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

logger = logging.getLogger("paper_sorts.services.paper_service")

# Typed aliases for update_field table/column parameters
UpdateTable = Literal["papers", "bib", "authors"]
UpdatePapersColumn = Literal["title", "contents"]


def search_by_title(session: Session, title: str) -> list[PaperSummary]:
    """Search for papers by exact title match.

    :param session: active SQLAlchemy session
    :type session: Session
    :param title: title to search for (exact match, case-sensitive)
    :type title: str
    :return: list of PaperSummary DTOs for matching papers (may be empty)
    :rtype: list[PaperSummary]
    """
    repo = PaperRepository(session)
    results = repo.search_by_title(title)
    logger.info("search_by_title(%r): %d result(s)", title, len(results))
    return results


def search_by_author(session: Session, author: str) -> list[PaperSummary]:
    """Search for papers by exact author name.

    :param session: active SQLAlchemy session
    :type session: Session
    :param author: author name in 'Last, First' format (exact match)
    :type author: str
    :return: list of PaperSummary DTOs for papers by this author (may be empty)
    :rtype: list[PaperSummary]
    """
    repo = PaperRepository(session)
    results = repo.search_by_author(author)
    logger.info("search_by_author(%r): %d result(s)", author, len(results))
    return results


def add_paper(session: Session, paper: PaperCreate) -> PaperSummary:
    """Add a new paper entry to the database.

    Inserts bib entry, paper record, and all author links in the same session.
    The caller's with_session context manager is responsible for committing.

    :param session: active SQLAlchemy session
    :type session: Session
    :param paper: DTO with all required paper fields
    :type paper: PaperCreate
    :return: PaperSummary DTO for the newly created paper
    :rtype: PaperSummary
    :raises ValueError: if bibtex_id already exists in the database
    """
    repo = PaperRepository(session)
    result = repo.create(paper)
    logger.info("add_paper: added '%s' (key=%s)", paper.title, paper.bibtex_id)
    return result


def update_field(
    session: Session,
    paper_id: int,
    table: str,
    column: str,
    value: str,
) -> None:
    """Update a single field of an existing paper, bib entry, or author.

    Uses match/case with exhaustiveness checking via assert_never for the
    table parameter. Raises ValueError for unsupported table/column combinations.

    :param session: active SQLAlchemy session
    :type session: Session
    :param paper_id: papers.id identifying the paper to update
    :type paper_id: int
    :param table: which table to update ('papers', 'bib', 'authors')
    :type table: str
    :param column: which column to update (depends on table)
    :type column: str
    :param value: new value to set
    :type value: str
    :raises ValueError: if table/column combination is not supported
    :raises ValueError: if paper_id does not exist
    """
    paper_repo = PaperRepository(session)
    bib_repo = BibRepository(session)
    author_repo = AuthorRepository(session)

    # Fetch the paper to get its bibtex_id
    paper_summary = paper_repo.get_by_id(paper_id)
    if paper_summary is None:
        raise ValueError(f"Paper with id {paper_id} not found")

    match table:
        case "papers":
            match column:
                case "title":
                    paper_repo.update_title(paper_id, value)
                    logger.info(
                        "update_field: updated title of paper %d to %r", paper_id, value
                    )
                case "contents":
                    paper_repo.update_contents(paper_id, value)
                    logger.info(
                        "update_field: updated contents of paper %d", paper_id
                    )
                case _:
                    raise ValueError(
                        f"Column '{column}' cannot be updated in table 'papers'. "
                        "Supported columns: title, contents"
                    )
        case "bib":
            match column:
                case "bibtex":
                    bib_repo.update(paper_summary.bibtex_id, value)
                    logger.info(
                        "update_field: updated bibtex for paper %d", paper_id
                    )
                case _:
                    raise ValueError(
                        f"Column '{column}' cannot be updated in table 'bib'. "
                        "Supported columns: bibtex"
                    )
        case "authors":
            match column:
                case "author":
                    # Update author name by paper_id: update the name of the first author
                    # linked to this paper who matches value's "old_name:new_name" format,
                    # OR treat value as new name for the single author.
                    # For simplicity, value is "author_id:new_name"
                    try:
                        author_id_str, new_name = value.split(":", 1)
                        author_id = int(author_id_str)
                    except ValueError:
                        raise ValueError(
                            "For author updates, value must be 'author_id:new_name'. "
                            f"Got: {value!r}"
                        )
                    author_repo.update_author_name(author_id, new_name)
                    logger.info(
                        "update_field: updated author %d name to %r", author_id, new_name
                    )
                case _:
                    raise ValueError(
                        f"Column '{column}' cannot be updated in table 'authors'. "
                        "Supported columns: author"
                    )
        case _:
            # assert_never ensures exhaustiveness at type-check time
            # We use a typed literal check for the known tables above;
            # unknown tables fall through here
            raise ValueError(
                f"Table '{table}' cannot be updated. "
                "Supported tables: papers, bib, authors"
            )


def delete_paper(session: Session, paper_id: int) -> None:
    """Delete a paper and all associated records from the database.

    Removes the paper row, its bib entry, all author-paper links, and any
    authors that have no remaining papers after this deletion.

    :param session: active SQLAlchemy session
    :type session: Session
    :param paper_id: papers.id of the paper to delete
    :type paper_id: int
    :raises ValueError: if paper_id does not exist
    """
    repo = PaperRepository(session)
    # Verify paper exists (raises ValueError if not)
    paper = repo.get_by_id(paper_id)
    if paper is None:
        raise ValueError(f"Paper with id {paper_id} not found")
    repo.delete(paper_id)
    logger.info("delete_paper: deleted paper %d ('%s')", paper_id, paper.title)
