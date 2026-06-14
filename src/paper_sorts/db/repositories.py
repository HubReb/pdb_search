"""Repository classes and Pydantic DTOs for the paper_sorts persistence layer.

This module is the ONLY place (besides models.py and session.py) that imports
sqlalchemy — constitution Principle I enforces driver isolation to src/paper_sorts/db/.

Architecture:
    Services call repository methods passing a Session obtained from with_session().
    Repositories translate between domain DTOs (PaperCreate, PaperSummary) and ORM objects.
    No I/O, no CLI, no config imports here.
"""

from __future__ import annotations

import logging
from typing import Literal

from pydantic import BaseModel
from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from paper_sorts.db.models import Author, AuthorPaper, Bib, Paper

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data Transfer Objects
# ---------------------------------------------------------------------------


class PaperCreate(BaseModel):
    """DTO used when inserting a new paper (add subcommand or bulk import).

    Attributes:
        title: Publication title.
        contents: One-sentence summary.
        bibtex_id: Unique BibTeX citation key.
        bibtex: Full BibTeX source string.
        authors: List of author names in 'Last, First' format.
    """

    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: list[str]


class PaperSummary(BaseModel):
    """DTO returned by search queries.

    Attributes:
        paper_id: Internal serial ID from the papers table.
        title: Publication title.
        contents: One-sentence summary.
        bibtex_id: BibTeX citation key.
        bibtex: Full BibTeX source string.
        authors: " and "-joined display string of all authors.
    """

    paper_id: int
    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: str


# ---------------------------------------------------------------------------
# BibRepository
# ---------------------------------------------------------------------------


class BibRepository:
    """CRUD operations for the bib table.

    All methods require a Session argument obtained from db.session.with_session().
    """

    @staticmethod
    def add(session: Session, bibtex_id: str, bibtex: str) -> None:
        """Insert a new BibTeX entry.

        Args:
            session: Active SQLAlchemy Session.
            bibtex_id: Primary key — the BibTeX citation key.
            bibtex: Full BibTeX source string (must be unique).

        Raises:
            ValueError: If bibtex_id already exists or bibtex string is a duplicate.
        """
        entry = Bib(bibtex_id=bibtex_id, bibtex=bibtex)
        session.add(entry)
        try:
            session.flush()
        except IntegrityError as exc:
            session.rollback()
            raise ValueError(
                f"BibTeX key {bibtex_id!r} or bibtex string already exists in database."
            ) from exc

    @staticmethod
    def get(session: Session, bibtex_id: str) -> str | None:
        """Return the bibtex string for the given key, or None if not found.

        Args:
            session: Active SQLAlchemy Session.
            bibtex_id: BibTeX citation key to look up.

        Returns:
            The BibTeX source string, or None if the key is not in the database.
        """
        entry = session.get(Bib, bibtex_id)
        return entry.bibtex if entry else None

    @staticmethod
    def update(session: Session, bibtex_id: str, new_bibtex: str) -> None:
        """Update the bibtex string for the given key.

        Args:
            session: Active SQLAlchemy Session.
            bibtex_id: BibTeX citation key identifying the entry to update.
            new_bibtex: New BibTeX source string.

        Raises:
            ValueError: If bibtex_id is not found, or if new_bibtex already exists
                (unique constraint violation).
        """
        entry = session.get(Bib, bibtex_id)
        if entry is None:
            raise ValueError(f"BibTeX key {bibtex_id!r} not found in bib table.")

        # Check unique constraint before update
        existing = session.execute(
            select(Bib).where(Bib.bibtex == new_bibtex)
        ).scalar_one_or_none()
        if existing is not None:
            raise ValueError(
                "bibtex is unique — the provided bibtex string already exists in the database."
            )

        entry.bibtex = new_bibtex
        session.flush()


# ---------------------------------------------------------------------------
# AuthorRepository
# ---------------------------------------------------------------------------


class AuthorRepository:
    """Operations on authors_id and authors_papers tables."""

    @staticmethod
    def get_or_create(session: Session, name: str) -> int:
        """Return the author_id for *name*, creating the author if necessary.

        Args:
            session: Active SQLAlchemy Session.
            name: Author name string in 'Last, First' format.

        Returns:
            The integer id from authors_id.
        """
        existing = session.execute(
            select(Author).where(Author.author == name)
        ).scalar_one_or_none()
        if existing is not None:
            return existing.id

        new_author = Author(author=name)
        session.add(new_author)
        session.flush()
        return new_author.id

    @staticmethod
    def link_to_paper(session: Session, author_id: int, paper_id: int) -> None:
        """Insert a row in authors_papers linking author to paper.

        Duplicates are silently ignored (same author_id + paper_id already linked).

        Args:
            session: Active SQLAlchemy Session.
            author_id: ID from authors_id.
            paper_id: ID from papers.
        """
        # Check for duplicate link
        existing = session.execute(
            select(AuthorPaper).where(
                AuthorPaper.author_id == author_id,
                AuthorPaper.paper_id == paper_id,
            )
        ).scalar_one_or_none()
        if existing is not None:
            return
        link = AuthorPaper(author_id=author_id, paper_id=paper_id)
        session.add(link)
        session.flush()

    @staticmethod
    def unlink_from_paper(session: Session, author_id: int, paper_id: int) -> None:
        """Remove the authors_papers row for this author-paper pair.

        Args:
            session: Active SQLAlchemy Session.
            author_id: ID from authors_id.
            paper_id: ID from papers.
        """
        session.execute(
            delete(AuthorPaper).where(
                AuthorPaper.author_id == author_id,
                AuthorPaper.paper_id == paper_id,
            )
        )
        session.flush()

    @staticmethod
    def cleanup_orphan(session: Session, author_id: int) -> None:
        """Delete an author from authors_id if they have no remaining paper links.

        Args:
            session: Active SQLAlchemy Session.
            author_id: ID from authors_id to potentially delete.
        """
        links = session.execute(
            select(AuthorPaper).where(AuthorPaper.author_id == author_id)
        ).scalars().all()
        if not links:
            session.execute(delete(Author).where(Author.id == author_id))
            session.flush()


# ---------------------------------------------------------------------------
# PaperRepository
# ---------------------------------------------------------------------------


class PaperRepository:
    """High-level operations on the papers table, joining with bib and authors."""

    @staticmethod
    def add(session: Session, paper: PaperCreate) -> int:
        """Insert a new paper with its BibTeX entry and author links.

        Adds rows to bib, papers, authors_id (if new), and authors_papers.
        All changes are flushed to the session but the caller's context manager
        controls the final commit.

        Args:
            session: Active SQLAlchemy Session.
            paper: PaperCreate DTO with all fields populated.

        Returns:
            The new paper's serial id from the papers table.

        Raises:
            ValueError: If bibtex_id already exists in the database.
        """
        BibRepository.add(session, paper.bibtex_id, paper.bibtex)

        new_paper = Paper(
            title=paper.title,
            contents=paper.contents,
            bibtex_id=paper.bibtex_id,
        )
        session.add(new_paper)
        session.flush()
        paper_id: int = new_paper.id

        for author_name in paper.authors:
            author_id = AuthorRepository.get_or_create(session, author_name)
            AuthorRepository.link_to_paper(session, author_id, paper_id)

        logger.info("Added paper %r (id=%d)", paper.title, paper_id)
        return paper_id

    @staticmethod
    def _build_summary(
        session: Session,
        paper: Paper,
    ) -> PaperSummary | None:
        """Build a PaperSummary DTO from an ORM Paper object.

        Args:
            session: Active SQLAlchemy Session.
            paper: ORM Paper instance.

        Returns:
            PaperSummary if bib entry and authors are found, None otherwise.
        """
        if paper.bibtex_id is None or paper.id is None:
            return None

        bib_entry = session.get(Bib, paper.bibtex_id)
        if bib_entry is None:
            return None

        # Gather authors
        links = session.execute(
            select(AuthorPaper).where(AuthorPaper.paper_id == paper.id)
        ).scalars().all()
        author_names: list[str] = []
        for link in links:
            author = session.get(Author, link.author_id)
            if author and author.author:
                author_names.append(author.author)

        return PaperSummary(
            paper_id=paper.id,
            title=paper.title or "",
            contents=paper.contents or "",
            bibtex_id=paper.bibtex_id,
            bibtex=bib_entry.bibtex,
            authors=" and ".join(author_names),
        )

    @staticmethod
    def get_by_title(session: Session, title: str) -> list[PaperSummary]:
        """Return all papers whose title matches exactly.

        Args:
            session: Active SQLAlchemy Session.
            title: Exact title string to search for.

        Returns:
            List of PaperSummary DTOs (empty list if none found).
        """
        papers = session.execute(
            select(Paper).where(Paper.title == title)
        ).scalars().all()

        results: list[PaperSummary] = []
        for paper in papers:
            summary = PaperRepository._build_summary(session, paper)
            if summary:
                results.append(summary)

        logger.info("search_by_title(%r) → %d result(s)", title, len(results))
        return results

    @staticmethod
    def get_by_author(session: Session, author: str) -> list[PaperSummary]:
        """Return all papers by the named author.

        Args:
            session: Active SQLAlchemy Session.
            author: Author name string in 'Last, First' format.

        Returns:
            List of PaperSummary DTOs (empty list if author not found).
        """
        author_row = session.execute(
            select(Author).where(Author.author == author)
        ).scalar_one_or_none()

        if author_row is None:
            logger.info("search_by_author(%r) → author not found", author)
            return []

        links = session.execute(
            select(AuthorPaper).where(AuthorPaper.author_id == author_row.id)
        ).scalars().all()

        results: list[PaperSummary] = []
        seen_ids: set[int] = set()
        for link in links:
            if link.paper_id is None or link.paper_id in seen_ids:
                continue
            paper = session.get(Paper, link.paper_id)
            if paper is None:
                continue
            summary = PaperRepository._build_summary(session, paper)
            if summary:
                results.append(summary)
                seen_ids.add(link.paper_id)

        logger.info("search_by_author(%r) → %d result(s)", author, len(results))
        return results

    @staticmethod
    def delete(session: Session, paper_id: int) -> None:
        """Delete a paper and clean up author links and orphaned authors.

        Removes rows from authors_papers, then papers, then bib. Orphaned
        authors (with no remaining paper links) are removed from authors_id.

        Args:
            session: Active SQLAlchemy Session.
            paper_id: Serial ID of the paper to delete.

        Raises:
            ValueError: If paper_id is not found in the database.
        """
        paper = session.get(Paper, paper_id)
        if paper is None:
            raise ValueError(f"Paper with id={paper_id} not found in database.")

        bibtex_id = paper.bibtex_id

        # Collect author IDs before deleting links
        links = session.execute(
            select(AuthorPaper).where(AuthorPaper.paper_id == paper_id)
        ).scalars().all()
        author_ids = [link.author_id for link in links if link.author_id is not None]

        # Remove author-paper links
        session.execute(delete(AuthorPaper).where(AuthorPaper.paper_id == paper_id))
        session.flush()

        # Cleanup orphaned authors
        for author_id in author_ids:
            AuthorRepository.cleanup_orphan(session, author_id)

        # Delete paper
        session.delete(paper)
        session.flush()

        # Delete bib entry if no longer referenced
        if bibtex_id:
            remaining = session.execute(
                select(Paper).where(Paper.bibtex_id == bibtex_id)
            ).first()
            if remaining is None:
                bib = session.get(Bib, bibtex_id)
                if bib:
                    session.delete(bib)
                    session.flush()

        logger.info("Deleted paper id=%d", paper_id)

    @staticmethod
    def update_field(
        session: Session,
        table: Literal["papers", "bib", "authors_id"],
        column: str,
        identifier: str | int,
        value: str,
    ) -> None:
        """Update a single field in the specified table.

        Args:
            session: Active SQLAlchemy Session.
            table: Target table name; must be one of 'papers', 'bib', 'authors_id'.
            column: Column to update.
            identifier: Row identifier (paper id for papers, bibtex_id for bib,
                author name for authors_id).
            value: New value to set.

        Raises:
            ValueError: If the table/column combination is not supported, if
                identifiers are not found, or on unique constraint violations.
        """
        match table:
            case "papers":
                PaperRepository._update_papers(session, column, int(identifier), value)
            case "bib":
                PaperRepository._update_bib(session, column, str(identifier), value)
            case "authors_id":
                PaperRepository._update_author(session, column, str(identifier), value)
            case _:
                raise ValueError(
                    f"Unknown table {table!r}. Must be one of 'papers', 'bib', 'authors_id'."
                )

    @staticmethod
    def _update_papers(session: Session, column: str, paper_id: int, value: str) -> None:
        """Update a column in the papers table identified by paper_id.

        Args:
            session: Active SQLAlchemy Session.
            column: Column to update ('title' or 'contents').
            paper_id: ID from papers table.
            value: New value.

        Raises:
            ValueError: If column is not updatable or paper_id not found.
        """
        paper = session.get(Paper, paper_id)
        if paper is None:
            raise ValueError(f"Paper with id={paper_id} not found.")
        match column:
            case "title":
                paper.title = value
            case "contents":
                paper.contents = value
            case _:
                raise ValueError(f"Column {column!r} is not updatable in table 'papers'.")
        session.flush()

    @staticmethod
    def _update_bib(session: Session, column: str, bibtex_id: str, value: str) -> None:
        """Update a column in the bib table identified by bibtex_id.

        Args:
            session: Active SQLAlchemy Session.
            column: Column to update (only 'bibtex' is supported).
            bibtex_id: BibTeX key identifying the entry.
            value: New bibtex string.

        Raises:
            ValueError: If column is not 'bibtex', or if value already exists,
                or if bibtex_id not found.
        """
        match column:
            case "bibtex":
                BibRepository.update(session, bibtex_id, value)
            case _:
                raise ValueError(f"Column {column!r} is not updatable in table 'bib'.")

    @staticmethod
    def _update_author(session: Session, column: str, old_name: str, new_name: str) -> None:
        """Rename an author in the database.

        If new_name already exists, re-links all papers from old_name to new_name
        and deletes the old entry. If new_name is new, renames the old entry directly.

        Args:
            session: Active SQLAlchemy Session.
            column: Must be 'author'.
            old_name: Current author name.
            new_name: Replacement author name.

        Raises:
            ValueError: If column is not 'author', or if old_name is not found.
        """
        match column:
            case "author":
                pass
            case _:
                raise ValueError(f"Column {column!r} is not updatable in table 'authors_id'.")

        old_author = session.execute(
            select(Author).where(Author.author == old_name)
        ).scalar_one_or_none()
        if old_author is None:
            raise ValueError(f"Author {old_name!r} not found in authors_id.")

        new_author = session.execute(
            select(Author).where(Author.author == new_name)
        ).scalar_one_or_none()

        if new_author is not None:
            # Re-link existing author's papers, removing duplicates
            new_id: int = new_author.id
            old_links = session.execute(
                select(AuthorPaper).where(AuthorPaper.author_id == old_author.id)
            ).scalars().all()
            for link in old_links:
                AuthorRepository.link_to_paper(session, new_id, link.paper_id or 0)
                session.delete(link)
            session.flush()
        else:
            # Simply rename the existing author entry
            old_author.author = new_name
            session.flush()
            # Delete old entry if it still exists as orphan
            return

        # Delete old author if they have no remaining links
        AuthorRepository.cleanup_orphan(session, old_author.id)
