"""Repository classes and data transfer objects for paper-sorts.

This module is the only persistence-layer interface exposed to the service
layer.  All SQLAlchemy usage is confined here (per constitution Principle I).

DTOs
----
:class:`PaperCreate` — data required to insert a new paper.
:class:`PaperSummary` — data returned from search/read operations.

Repositories
------------
:class:`BibRepository` — CRUD on the ``bib`` table.
:class:`AuthorRepository` — CRUD on ``authors_id`` and ``authors_papers``.
:class:`PaperRepository` — Full-stack CRUD on all four tables.
"""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from paper_sorts.db.models import Author, AuthorPaper, Bib, Paper

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data Transfer Objects
# ---------------------------------------------------------------------------


class PaperCreate(BaseModel):
    """Data required to create a new paper record.

    :param title: Publication title.
    :param authors: Author names in ``"Last, First"`` form.
    :param bibtex_key: BibTeX citation key (must be unique).
    :param summary: One-sentence summary of the paper.
    :param bibtex_text: Full BibTeX source string.
    """

    title: str
    authors: list[str]
    bibtex_key: str
    summary: str
    bibtex_text: str


class PaperSummary(BaseModel):
    """Summary of a paper record returned from search/read operations.

    :param paper_id: Internal surrogate key from ``papers.id``.
    :param title: Publication title.
    :param authors: Author names in ``"Last, First"`` form.
    :param summary: One-sentence summary.
    :param bibtex_key: BibTeX citation key.
    :param bibtex_text: Full BibTeX source string.
    """

    paper_id: int
    title: str
    authors: list[str]
    summary: str
    bibtex_key: str
    bibtex_text: str


# ---------------------------------------------------------------------------
# BibRepository
# ---------------------------------------------------------------------------


class BibRepository:
    """Repository for BibTeX entries (the ``bib`` table).

    All methods take an open :class:`sqlalchemy.orm.Session`; the caller owns
    the session lifecycle (open, commit, close).
    """

    @staticmethod
    def get_by_key(session: Session, key: str) -> Bib | None:
        """Return the :class:`Bib` with the given BibTeX key, or *None*.

        :param session: Active SQLAlchemy session.
        :param key: BibTeX citation key to look up.
        :returns: Matching :class:`Bib` row, or ``None`` if not found.
        """
        return session.get(Bib, key)

    @staticmethod
    def create(session: Session, key: str, text: str) -> Bib:
        """Insert a new :class:`Bib` row and return it.

        The row is added to the session but not committed; the caller commits.

        :param session: Active SQLAlchemy session.
        :param key: BibTeX citation key (must be unique in the table).
        :param text: Full BibTeX source string.
        :returns: The newly created :class:`Bib` instance (not yet committed).
        :raises sqlalchemy.exc.IntegrityError: If *key* already exists.
        """
        bib = Bib(bibtex_id=key, bibtex=text)
        session.add(bib)
        session.flush()
        return bib


# ---------------------------------------------------------------------------
# AuthorRepository
# ---------------------------------------------------------------------------


class AuthorRepository:
    """Repository for author names and paper–author links.

    Manages the ``authors_id`` and ``authors_papers`` tables.
    """

    @staticmethod
    def get_or_create(session: Session, name: str) -> Author:
        """Return an existing :class:`Author` by name, or create a new one.

        Author deduplication is by string equality (documented limitation:
        two strings that represent the same person are treated as distinct
        authors).

        :param session: Active SQLAlchemy session.
        :param name: Author name in ``"Last, First"`` form.
        :returns: Existing or newly created :class:`Author` instance.
        """
        stmt = select(Author).where(Author.author == name)
        author = session.execute(stmt).scalars().first()
        if author is None:
            author = Author(author=name)
            session.add(author)
            session.flush()
        return author

    @staticmethod
    def get_by_paper_id(session: Session, paper_id: int) -> list[Author]:
        """Return all :class:`Author` rows linked to *paper_id*.

        :param session: Active SQLAlchemy session.
        :param paper_id: ``papers.id`` to look up authors for.
        :returns: List of :class:`Author` rows (may be empty).
        """
        stmt = (
            select(Author)
            .join(AuthorPaper, Author.id == AuthorPaper.author_id)
            .where(AuthorPaper.paper_id == paper_id)
        )
        return list(session.execute(stmt).scalars().all())

    @staticmethod
    def link(session: Session, author_id: int, paper_id: int) -> None:
        """Insert a link row in ``authors_papers``.

        :param session: Active SQLAlchemy session.
        :param author_id: ``authors_id.id`` of the author.
        :param paper_id: ``papers.id`` of the paper.
        """
        link = AuthorPaper(author_id=author_id, paper_id=paper_id)
        session.add(link)
        session.flush()

    @staticmethod
    def unlink_paper(session: Session, paper_id: int) -> None:
        """Remove all ``authors_papers`` rows for *paper_id*.

        :param session: Active SQLAlchemy session.
        :param paper_id: ``papers.id`` whose author links should be removed.
        """
        stmt = select(AuthorPaper).where(AuthorPaper.paper_id == paper_id)
        for link in session.execute(stmt).scalars().all():
            session.delete(link)
        session.flush()


# ---------------------------------------------------------------------------
# PaperRepository
# ---------------------------------------------------------------------------


def _build_summary(session: Session, paper: Paper) -> PaperSummary:
    """Build a :class:`PaperSummary` from a :class:`Paper` ORM row.

    Fetches the associated :class:`Bib` row and author list from the
    database.

    :param session: Active SQLAlchemy session.
    :param paper: ORM :class:`Paper` row.
    :returns: Fully populated :class:`PaperSummary`.
    :raises KeyError: If the :class:`Bib` row referenced by the paper is
        missing (data integrity problem).
    """
    bib = session.get(Bib, paper.bibtex_id)
    if bib is None:
        raise KeyError(f"Bib row not found for bibtex_id={paper.bibtex_id!r}")
    authors = AuthorRepository.get_by_paper_id(session, paper.id)
    author_names = [a.author or "" for a in authors]
    return PaperSummary(
        paper_id=paper.id,
        title=paper.title or "",
        authors=author_names,
        summary=paper.contents or "",
        bibtex_key=paper.bibtex_id or "",
        bibtex_text=bib.bibtex,
    )


class PaperRepository:
    """Full-stack CRUD repository for papers.

    Coordinates ``papers``, ``bib``, ``authors_id``, and ``authors_papers``
    tables.  All methods take an open session; the caller controls commits.
    """

    @staticmethod
    def search_by_title(session: Session, title: str) -> list[PaperSummary]:
        """Return all papers whose title matches *title* (case-sensitive).

        :param session: Active SQLAlchemy session.
        :param title: Exact title string to search for.
        :returns: List of matching :class:`PaperSummary` objects (may be empty).
        """
        stmt = select(Paper).where(Paper.title == title)
        papers = session.execute(stmt).scalars().all()
        return [_build_summary(session, p) for p in papers]

    @staticmethod
    def search_by_author(session: Session, author: str) -> list[PaperSummary]:
        """Return all papers by an author whose name matches *author*.

        :param session: Active SQLAlchemy session.
        :param author: Exact author name in ``"Last, First"`` form.
        :returns: List of matching :class:`PaperSummary` objects (may be empty).
        """
        stmt = (
            select(Paper)
            .join(AuthorPaper, Paper.id == AuthorPaper.paper_id)
            .join(Author, Author.id == AuthorPaper.author_id)
            .where(Author.author == author)
        )
        papers = session.execute(stmt).scalars().all()
        return [_build_summary(session, p) for p in papers]

    @staticmethod
    def get_by_id(session: Session, paper_id: int) -> PaperSummary | None:
        """Return the paper with *paper_id*, or *None* if not found.

        :param session: Active SQLAlchemy session.
        :param paper_id: ``papers.id`` to retrieve.
        :returns: :class:`PaperSummary`, or ``None`` if not found.
        """
        paper = session.get(Paper, paper_id)
        if paper is None:
            return None
        return _build_summary(session, paper)

    @staticmethod
    def create(session: Session, data: PaperCreate) -> PaperSummary:
        """Insert a new paper (bib entry + paper row + author links).

        Inserts in order: bib → paper → author links.  Callers should wrap
        this in a ``with_session`` context so the whole insert is atomic.

        :param session: Active SQLAlchemy session.
        :param data: :class:`PaperCreate` DTO with all required fields.
        :returns: :class:`PaperSummary` for the newly created paper.
        :raises sqlalchemy.exc.IntegrityError: If *bibtex_key* already exists
            in the ``bib`` table.
        """
        BibRepository.create(session, data.bibtex_key, data.bibtex_text)
        paper = Paper(
            title=data.title,
            contents=data.summary,
            bibtex_id=data.bibtex_key,
        )
        session.add(paper)
        session.flush()
        for name in data.authors:
            author = AuthorRepository.get_or_create(session, name)
            AuthorRepository.link(session, author.id, paper.id)
        return _build_summary(session, paper)

    @staticmethod
    def update_field(
        session: Session, paper_id: int, field: str, value: str
    ) -> None:
        """Update a single field on an existing paper.

        Supported fields: ``"title"``, ``"contents"``, ``"bibtex"``,
        ``"author"``.

        - ``"title"`` → updates ``papers.title``
        - ``"contents"`` → updates ``papers.contents``
        - ``"bibtex"`` → updates ``bib.bibtex`` for the linked bib row
        - ``"author"`` → replaces all author links with a single new author

        :param session: Active SQLAlchemy session.
        :param paper_id: ``papers.id`` of the paper to update.
        :param field: Name of the field to update.  Must be one of the
            supported values listed above.
        :param value: New value for the field.
        :raises ValueError: If *paper_id* is not found or *field* is invalid.
        """
        paper = session.get(Paper, paper_id)
        if paper is None:
            raise ValueError(f"Paper {paper_id} not found")
        if field == "title":
            paper.title = value
        elif field == "contents":
            paper.contents = value
        elif field == "bibtex":
            bib = session.get(Bib, paper.bibtex_id)
            if bib is None:
                raise ValueError(
                    f"Bib row not found for paper {paper_id}"
                )
            bib.bibtex = value
        elif field == "author":
            AuthorRepository.unlink_paper(session, paper_id)
            author = AuthorRepository.get_or_create(session, value)
            AuthorRepository.link(session, author.id, paper_id)
        else:
            raise ValueError(f"Unknown field: {field!r}")
        session.flush()

    @staticmethod
    def delete(session: Session, paper_id: int) -> None:
        """Delete a paper and all its author links.

        Removes author links from ``authors_papers``, then removes the
        ``papers`` row.  The ``bib`` row is left in place (other papers may
        reference the same BibTeX key).

        :param session: Active SQLAlchemy session.
        :param paper_id: ``papers.id`` of the paper to delete.
        :raises ValueError: If *paper_id* is not found.
        """
        paper = session.get(Paper, paper_id)
        if paper is None:
            raise ValueError(f"Paper {paper_id} not found")
        AuthorRepository.unlink_paper(session, paper_id)
        session.delete(paper)
        session.flush()


# ---------------------------------------------------------------------------
# Helper used by services and CLI
# ---------------------------------------------------------------------------


def papers_to_display(summaries: list[PaperSummary]) -> list[dict[str, Any]]:
    """Convert a list of :class:`PaperSummary` objects to display dicts.

    :param summaries: List of paper summaries.
    :returns: List of dicts with keys ``paper_id``, ``title``, ``authors``,
        ``summary``, ``bibtex_key``, ``bibtex_text``.
    """
    return [s.model_dump() for s in summaries]
