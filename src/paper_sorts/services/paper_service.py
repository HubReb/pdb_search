"""High-level domain operations for paper management.

This module is the single service layer between the CLI and the persistence
layer. It orchestrates repository calls within a session context.

Constraints (constitution Principle I):
- No sqlalchemy imports — interact with the database only via repositories.
- No rich, no I/O — pure orchestration returning DTOs.
- No config imports — db_url is provided by the caller (CLI).
"""

from __future__ import annotations

import logging
from typing import Literal

from paper_sorts.db.repositories import PaperCreate, PaperRepository, PaperSummary
from paper_sorts.db.session import with_session

logger = logging.getLogger(__name__)


def search_by_title(db_url: str, title: str) -> list[PaperSummary]:
    """Search the database for papers whose title matches exactly.

    Args:
        db_url: SQLAlchemy-compatible database URL.
        title: Exact paper title to search for.

    Returns:
        List of PaperSummary DTOs (empty list if none found).
    """
    with with_session(db_url) as session:
        return PaperRepository.get_by_title(session, title)


def search_by_author(db_url: str, author: str) -> list[PaperSummary]:
    """Search the database for all papers by the named author.

    Args:
        db_url: SQLAlchemy-compatible database URL.
        author: Author name in 'Last, First' format.

    Returns:
        List of PaperSummary DTOs (empty list if author not found).
    """
    with with_session(db_url) as session:
        return PaperRepository.get_by_author(session, author)


def add_paper(db_url: str, paper: PaperCreate) -> None:
    """Insert a new paper into the database.

    Args:
        db_url: SQLAlchemy-compatible database URL.
        paper: PaperCreate DTO with all required fields populated.

    Raises:
        ValueError: If the bibtex_id already exists in the database.
    """
    with with_session(db_url) as session:
        paper_id = PaperRepository.add(session, paper)
    logger.info("Paper %r added with id=%d", paper.title, paper_id)


def update_field(
    db_url: str,
    table: Literal["papers", "bib", "authors_id"],
    column: str,
    identifier: str | int,
    value: str,
) -> None:
    """Update a single field in the specified database table.

    Args:
        db_url: SQLAlchemy-compatible database URL.
        table: Target table; one of 'papers', 'bib', 'authors_id'.
        column: Column name to update.
        identifier: Row key — paper id (int) for papers, bibtex_id (str) for bib,
            author name (str) for authors_id.
        value: New value to write.

    Raises:
        ValueError: If the table/column combination is not supported, if the
            identified row is not found, or on unique constraint violations.
    """
    with with_session(db_url) as session:
        PaperRepository.update_field(session, table, column, identifier, value)
    logger.info("Updated %s.%s (id=%r) → %r", table, column, identifier, value)


def delete_paper(db_url: str, paper_id: int) -> None:
    """Delete a paper and all associated data from the database.

    Removes the paper from papers, its BibTeX entry from bib (if unreferenced),
    and all author links from authors_papers. Orphaned authors are removed from
    authors_id.

    Args:
        db_url: SQLAlchemy-compatible database URL.
        paper_id: Serial ID of the paper to delete.

    Raises:
        ValueError: If the paper_id is not found in the database.
    """
    with with_session(db_url) as session:
        PaperRepository.delete(session, paper_id)
    logger.info("Deleted paper id=%d", paper_id)
