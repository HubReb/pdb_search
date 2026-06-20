"""Repository classes and Pydantic DTOs for the paper_sorts persistence layer.

Only this module (and ``db/models.py``, ``db/session.py``) may import SQLAlchemy.
Service-layer code interacts with the database exclusively via the repository
classes and the DTO types exported here.

DTOs
----
- :class:`PaperCreate` — write model; passed to :meth:`PaperRepository.add_paper`.
- :class:`PaperSummary` — read model; returned by search methods.

Repositories
------------
- :class:`PaperRepository` — CRUD on ``papers`` + joined results.
- :class:`AuthorRepository` — author lookup, creation, name update.
- :class:`BibRepository` — BibTeX entry read/update.
"""

from __future__ import annotations

import logging

from pydantic import BaseModel
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from paper_sorts.db.models import Author, AuthorPaper, Bib, Paper

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data Transfer Objects
# ---------------------------------------------------------------------------


class PaperCreate(BaseModel):
    """Write DTO for adding a new paper to the database.

    :param title: Publication title.
    :param contents: One-sentence summary of the paper.
    :param bibtex_id: Unique BibTeX citation key.
    :param bibtex: Full BibTeX source string.
    :param authors: Non-empty list of author names in ``"Last, First"`` form.
    """

    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: list[str]


class PaperSummary(BaseModel):
    """Read DTO returned by search operations.

    :param paper_id: Internal ``papers.id``.
    :param title: Publication title.
    :param contents: One-sentence summary.
    :param bibtex_id: Unique BibTeX citation key.
    :param authors: Author names joined from ``authors_id``.
    :param bibtex: Full BibTeX source string from ``bib``.
    """

    paper_id: int
    title: str
    contents: str
    bibtex_id: str
    authors: list[str]
    bibtex: str


# ---------------------------------------------------------------------------
# PaperRepository
# ---------------------------------------------------------------------------


class PaperRepository:
    """High-level persistence operations for papers.

    All methods accept an open :class:`sqlalchemy.orm.Session` and operate
    within its transaction; callers are responsible for committing or rolling
    back via :func:`~paper_sorts.db.session.with_session`.
    """

    @staticmethod
    def _build_summary(
        session: Session, paper: Paper, authors: list[str]
    ) -> PaperSummary:
        """Construct a :class:`PaperSummary` for *paper* with *authors*.

        :param session: Active database session (used to fetch BibTeX entry).
        :param paper: ORM :class:`Paper` instance.
        :param authors: Pre-fetched list of author names.
        :returns: A populated :class:`PaperSummary`.
        :raises ValueError: If BibTeX record for *paper* is missing.
        """
        bib = session.get(Bib, paper.bibtex_id)
        if bib is None:
            raise ValueError(f"BibTeX record missing for bibtex_id={paper.bibtex_id!r}")
        return PaperSummary(
            paper_id=paper.id,
            title=paper.title or "",
            contents=paper.contents or "",
            bibtex_id=paper.bibtex_id or "",
            authors=authors,
            bibtex=bib.bibtex,
        )

    @staticmethod
    def _get_authors_for_paper(session: Session, paper_id: int) -> list[str]:
        """Return author names for *paper_id*, joined from ``authors_id``.

        :param session: Active database session.
        :param paper_id: ``papers.id`` value.
        :returns: List of author name strings (may be empty).
        """
        stmt = (
            select(Author.author)
            .join(AuthorPaper, AuthorPaper.author_id == Author.id)
            .where(AuthorPaper.paper_id == paper_id)
            .order_by(Author.id)
        )
        return list(session.scalars(stmt))

    @classmethod
    def search_by_title(cls, session: Session, title: str) -> list[PaperSummary]:
        """Return all papers whose title exactly matches *title*.

        :param session: Active database session.
        :param title: Exact title string to search for.
        :returns: List of :class:`PaperSummary` (empty if not found).
        """
        stmt = select(Paper).where(Paper.title == title)
        papers = list(session.scalars(stmt))
        if not papers:
            logger.info("Paper with title %r not found", title)
            return []
        results: list[PaperSummary] = []
        for paper in papers:
            authors = cls._get_authors_for_paper(session, paper.id)
            results.append(cls._build_summary(session, paper, authors))
        return results

    @classmethod
    def search_by_author(cls, session: Session, author: str) -> list[PaperSummary]:
        """Return all papers attributed to *author*.

        :param session: Active database session.
        :param author: Exact author name in ``"Last, First"`` form.
        :returns: List of :class:`PaperSummary` (empty if not found).
        """
        stmt = (
            select(Paper)
            .join(AuthorPaper, AuthorPaper.paper_id == Paper.id)
            .join(Author, Author.id == AuthorPaper.author_id)
            .where(Author.author == author)
            .order_by(Paper.id)
        )
        papers = list(session.scalars(stmt))
        if not papers:
            logger.info("Author %r not found", author)
            return []
        results: list[PaperSummary] = []
        for paper in papers:
            authors = cls._get_authors_for_paper(session, paper.id)
            results.append(cls._build_summary(session, paper, authors))
        return results

    @staticmethod
    def add_paper(session: Session, paper: PaperCreate) -> None:
        """Persist a new paper (bib entry, paper row, author rows, link rows).

        :param session: Active database session.
        :param paper: :class:`PaperCreate` DTO with all required fields.
        :raises ValueError: If *paper.bibtex_id* already exists in ``bib``.
        :raises ValueError: If *paper.authors* is empty.
        """
        if not paper.authors:
            raise ValueError("Paper must have at least one author")
        existing = session.get(Bib, paper.bibtex_id)
        if existing is not None:
            raise ValueError(f"Entry already exists (key: {paper.bibtex_id!r})")

        bib_row = Bib(bibtex_id=paper.bibtex_id, bibtex=paper.bibtex)
        session.add(bib_row)
        session.flush()

        paper_row = Paper(
            title=paper.title,
            contents=paper.contents,
            bibtex_id=paper.bibtex_id,
        )
        session.add(paper_row)
        session.flush()

        for author_name in paper.authors:
            author = AuthorRepository.get_or_create_author(session, author_name)
            AuthorRepository.link_author_paper(session, author.id, paper_row.id)

    @staticmethod
    def delete_paper(session: Session, bibtex_id: str) -> None:
        """Delete a paper and all associated rows (authorship links, bib entry).

        Author rows in ``authors_id`` that have no remaining papers are also
        removed.

        :param session: Active database session.
        :param bibtex_id: The BibTeX citation key of the paper to delete.
        :raises ValueError: If no paper with *bibtex_id* exists.
        """
        paper = session.scalar(select(Paper).where(Paper.bibtex_id == bibtex_id))
        if paper is None:
            raise ValueError(f"Paper not found (key: {bibtex_id!r})")

        author_ids_stmt = select(AuthorPaper.author_id).where(
            AuthorPaper.paper_id == paper.id
        )
        author_ids = list(session.scalars(author_ids_stmt))

        session.execute(
            delete(AuthorPaper).where(AuthorPaper.paper_id == paper.id)
        )

        for author_id in author_ids:
            remaining = session.scalar(
                select(AuthorPaper).where(AuthorPaper.author_id == author_id)
            )
            if remaining is None:
                session.execute(delete(Author).where(Author.id == author_id))

        session.delete(paper)
        session.flush()

        bib = session.get(Bib, bibtex_id)
        if bib is not None:
            session.delete(bib)


# ---------------------------------------------------------------------------
# AuthorRepository
# ---------------------------------------------------------------------------


class AuthorRepository:
    """Persistence operations for author records."""

    @staticmethod
    def get_or_create_author(session: Session, name: str) -> Author:
        """Return the existing :class:`Author` for *name*, or create a new one.

        :param session: Active database session.
        :param name: Author name in ``"Last, First"`` form.
        :returns: The :class:`Author` ORM object (id is populated after flush).
        """
        existing = session.scalar(select(Author).where(Author.author == name))
        if existing is not None:
            return existing
        author = Author(author=name)
        session.add(author)
        session.flush()
        return author

    @staticmethod
    def link_author_paper(session: Session, author_id: int, paper_id: int) -> None:
        """Insert an ``authors_papers`` row linking *author_id* to *paper_id*.

        :param session: Active database session.
        :param author_id: ``authors_id.id`` value.
        :param paper_id: ``papers.id`` value.
        """
        link = AuthorPaper(author_id=author_id, paper_id=paper_id)
        session.add(link)
        session.flush()

    @staticmethod
    def unlink_author_paper(session: Session, author_id: int, paper_id: int) -> None:
        """Remove the ``authors_papers`` row for *(author_id, paper_id)*.

        :param session: Active database session.
        :param author_id: ``authors_id.id`` value.
        :param paper_id: ``papers.id`` value.
        """
        session.execute(
            delete(AuthorPaper).where(
                AuthorPaper.author_id == author_id,
                AuthorPaper.paper_id == paper_id,
            )
        )

    @staticmethod
    def update_author_name(session: Session, old_name: str, new_name: str) -> None:
        """Rename an author from *old_name* to *new_name*.

        If *new_name* already exists in ``authors_id``, the ``authors_papers``
        links of *old_name* are re-pointed to the existing record and the
        *old_name* row is deleted.  If *new_name* does not exist, the
        ``author`` column of the existing row is updated in place.

        :param session: Active database session.
        :param old_name: Current author name.
        :param new_name: Replacement author name.
        :raises ValueError: If *old_name* is not found in ``authors_id``.
        """
        old_author = session.scalar(select(Author).where(Author.author == old_name))
        if old_author is None:
            raise ValueError(f"Author {old_name!r} not found")

        new_author = session.scalar(select(Author).where(Author.author == new_name))

        if new_author is not None:
            # Re-point links and clean up duplicates
            session.execute(
                delete(AuthorPaper).where(
                    AuthorPaper.author_id == new_author.id,
                    AuthorPaper.paper_id.in_(
                        select(AuthorPaper.paper_id).where(
                            AuthorPaper.author_id == old_author.id
                        )
                    ),
                )
            )
            from sqlalchemy import update as sa_update

            session.execute(
                sa_update(AuthorPaper)
                .where(AuthorPaper.author_id == old_author.id)
                .values(author_id=new_author.id)
            )
            session.flush()
            session.delete(old_author)
        else:
            old_author.author = new_name

        session.flush()


# ---------------------------------------------------------------------------
# BibRepository
# ---------------------------------------------------------------------------


class BibRepository:
    """Persistence operations for BibTeX entries."""

    @staticmethod
    def get_bibtex(session: Session, bibtex_id: str) -> str | None:
        """Return the BibTeX string for *bibtex_id*, or ``None`` if not found.

        :param session: Active database session.
        :param bibtex_id: The BibTeX citation key.
        :returns: BibTeX source string or ``None``.
        """
        bib = session.get(Bib, bibtex_id)
        return bib.bibtex if bib is not None else None

    @staticmethod
    def update_bibtex(session: Session, bibtex_id: str, new_bibtex: str) -> None:
        """Replace the BibTeX string for *bibtex_id* with *new_bibtex*.

        :param session: Active database session.
        :param bibtex_id: The BibTeX citation key to update.
        :param new_bibtex: New BibTeX source string (must not duplicate an
            existing ``bibtex`` value — enforced by the UNIQUE constraint).
        :raises ValueError: If *bibtex_id* is not found.
        :raises ValueError: If *new_bibtex* already exists for another key.
        """
        bib = session.get(Bib, bibtex_id)
        if bib is None:
            raise ValueError(f"BibTeX entry {bibtex_id!r} not found")
        existing = session.scalar(select(Bib).where(Bib.bibtex == new_bibtex))
        if existing is not None and existing.bibtex_id != bibtex_id:
            raise ValueError("bibtex is unique — value already exists")
        bib.bibtex = new_bibtex
        session.flush()
