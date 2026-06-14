"""Repository classes and data-transfer objects for paper_sorts.

All database access is concentrated here.  Service-layer code receives and
returns :class:`PaperCreate` / :class:`PaperSummary` DTOs; it never imports
SQLAlchemy types directly.

Only modules under ``src/paper_sorts/db/`` may import SQLAlchemy.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pydantic import BaseModel
from sqlalchemy import select

from paper_sorts.db.models import Author, AuthorPaper, Bib, Paper

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data-Transfer Objects
# ---------------------------------------------------------------------------


class PaperCreate(BaseModel):
    """DTO for creating a new paper record.

    :param title: publication title.
    :param contents: one-sentence summary.
    :param bibtex_id: unique BibTeX cite key.
    :param bibtex: full BibTeX source string.
    :param authors: list of author names in ``"Last, First"`` form.
    """

    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: list[str]


class PaperSummary(BaseModel):
    """DTO returned by search operations.

    :param id: internal paper ID.
    :param title: publication title.
    :param contents: one-sentence summary.
    :param bibtex_id: unique BibTeX cite key.
    :param bibtex: full BibTeX source string.
    :param authors: list of author names in ``"Last, First"`` form.
    """

    id: int
    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: list[str]


# ---------------------------------------------------------------------------
# BibRepository
# ---------------------------------------------------------------------------


class BibRepository:
    """Repository for the ``bib`` table.

    All methods require an open :class:`sqlalchemy.orm.Session` passed by the
    caller; they do not open or commit sessions themselves.
    """

    @staticmethod
    def add_bib(session: Session, bibtex_id: str, bibtex: str) -> None:
        """Insert a new BibTeX entry.

        :param session: active SQLAlchemy session.
        :param bibtex_id: unique cite key.
        :param bibtex: full BibTeX source string.
        :raises IntegrityError: if ``bibtex_id`` or ``bibtex`` already exists.
        """
        bib = Bib(bibtex_id=bibtex_id, bibtex=bibtex)
        session.add(bib)
        session.flush()

    @staticmethod
    def get_bib(session: Session, bibtex_id: str) -> Bib | None:
        """Fetch a BibTeX entry by its cite key.

        :param session: active SQLAlchemy session.
        :param bibtex_id: cite key to look up.
        :returns: matching :class:`Bib` row, or ``None`` if not found.
        """
        return session.get(Bib, bibtex_id)

    @staticmethod
    def update_bib(session: Session, bibtex_id: str, new_bibtex: str) -> None:
        """Update the BibTeX source string for an existing entry.

        :param session: active SQLAlchemy session.
        :param bibtex_id: cite key of the entry to update.
        :param new_bibtex: new BibTeX source string.
        :raises ValueError: if no entry with ``bibtex_id`` exists, or if
            ``new_bibtex`` would violate the UNIQUE constraint on ``bibtex``.
        :raises IntegrityError: if ``new_bibtex`` is already stored for another entry.
        """
        bib = session.get(Bib, bibtex_id)
        if bib is None:
            raise ValueError(f"BibTeX entry '{bibtex_id}' not found.")
        # Check uniqueness manually so we can surface a user-friendly message.
        existing = session.scalar(
            select(Bib).where(Bib.bibtex == new_bibtex, Bib.bibtex_id != bibtex_id)
        )
        if existing is not None:
            raise ValueError(
                f"The BibTeX string is already stored under key '{existing.bibtex_id}'."
            )
        bib.bibtex = new_bibtex
        session.flush()

    @staticmethod
    def delete_bib(session: Session, bibtex_id: str) -> None:
        """Delete a BibTeX entry.

        :param session: active SQLAlchemy session.
        :param bibtex_id: cite key of the entry to delete.
        """
        bib = session.get(Bib, bibtex_id)
        if bib is not None:
            session.delete(bib)
            session.flush()


# ---------------------------------------------------------------------------
# AuthorRepository
# ---------------------------------------------------------------------------


class AuthorRepository:
    """Repository for the ``authors_id`` and ``authors_papers`` tables."""

    @staticmethod
    def get_or_create_author(session: Session, name: str) -> int:
        """Return the ID of an author by name, creating the row if absent.

        :param session: active SQLAlchemy session.
        :param name: author name in ``"Last, First"`` form.
        :returns: integer ID of the (possibly newly created) author row.
        """
        existing = session.scalar(select(Author).where(Author.author == name))
        if existing is not None:
            return existing.id
        author = Author(author=name)
        session.add(author)
        session.flush()
        return author.id

    @staticmethod
    def link_author_to_paper(session: Session, author_id: int, paper_id: int) -> None:
        """Create an entry in ``authors_papers`` linking author to paper.

        :param session: active SQLAlchemy session.
        :param author_id: ID from ``authors_id``.
        :param paper_id: ID from ``papers``.
        """
        link = AuthorPaper(author_id=author_id, paper_id=paper_id)
        session.add(link)
        session.flush()

    @staticmethod
    def get_authors_for_paper(session: Session, paper_id: int) -> list[str]:
        """Return the list of author names for a given paper.

        :param session: active SQLAlchemy session.
        :param paper_id: integer paper ID.
        :returns: list of author name strings in ``"Last, First"`` form.
        """
        rows = session.execute(
            select(Author.author)
            .join(AuthorPaper, AuthorPaper.author_id == Author.id)
            .where(AuthorPaper.paper_id == paper_id)
        ).scalars().all()
        return [r for r in rows if r is not None]

    @staticmethod
    def delete_links_for_paper(session: Session, paper_id: int) -> None:
        """Remove all ``authors_papers`` rows for *paper_id*.

        Authors that have no remaining paper links are also removed from
        ``authors_id`` (orphan cleanup).

        :param session: active SQLAlchemy session.
        :param paper_id: integer paper ID whose authorship links to remove.
        """
        links = session.scalars(
            select(AuthorPaper).where(AuthorPaper.paper_id == paper_id)
        ).all()
        author_ids = [lnk.author_id for lnk in links]
        for link in links:
            session.delete(link)
        session.flush()

        # Orphan cleanup: delete authors with no remaining paper links.
        for aid in author_ids:
            still_linked = session.scalar(
                select(AuthorPaper).where(AuthorPaper.author_id == aid)
            )
            if still_linked is None:
                author = session.get(Author, aid)
                if author is not None:
                    session.delete(author)
        session.flush()

    @staticmethod
    def update_author_name(session: Session, old_name: str, new_name: str) -> None:
        """Rename an author, merging into an existing author row if necessary.

        If *new_name* already exists in ``authors_id``, all ``authors_papers``
        links from *old_name* are re-pointed to the existing author, duplicate
        links are de-duplicated, and the *old_name* row is removed.  If
        *new_name* does not exist, the *old_name* row is simply renamed.

        :param session: active SQLAlchemy session.
        :param old_name: current author name to change.
        :param new_name: replacement author name.
        :raises ValueError: if no author with *old_name* exists.
        """
        old_author = session.scalar(select(Author).where(Author.author == old_name))
        if old_author is None:
            raise ValueError(f"Author '{old_name}' not found.")

        new_author = session.scalar(select(Author).where(Author.author == new_name))
        if new_author is not None:
            # Merge: re-point old links to the existing new author.
            old_links = session.scalars(
                select(AuthorPaper).where(AuthorPaper.author_id == old_author.id)
            ).all()
            for link in old_links:
                # Check whether the target paper already has the new author linked.
                dup = session.scalar(
                    select(AuthorPaper).where(
                        AuthorPaper.author_id == new_author.id,
                        AuthorPaper.paper_id == link.paper_id,
                    )
                )
                if dup is not None:
                    session.delete(link)
                else:
                    link.author_id = new_author.id
            session.flush()
            session.delete(old_author)
        else:
            # Simple rename.
            old_author.author = new_name
        session.flush()


# ---------------------------------------------------------------------------
# PaperRepository
# ---------------------------------------------------------------------------


class PaperRepository:
    """Repository for the ``papers`` table and related join queries."""

    @staticmethod
    def search_by_title(session: Session, title: str) -> list[PaperSummary]:
        """Search for papers matching *title* exactly.

        :param session: active SQLAlchemy session.
        :param title: exact title string to match.
        :returns: list of :class:`PaperSummary` objects (may be empty).
        """
        papers = session.scalars(
            select(Paper).where(Paper.title == title)
        ).all()
        return [
            PaperRepository._to_summary(session, p) for p in papers
        ]

    @staticmethod
    def search_by_author(session: Session, author: str) -> list[PaperSummary]:
        """Search for papers by exact author name.

        :param session: active SQLAlchemy session.
        :param author: exact author name string to match.
        :returns: list of :class:`PaperSummary` objects (may be empty).
        """
        paper_ids = session.scalars(
            select(AuthorPaper.paper_id)
            .join(Author, Author.id == AuthorPaper.author_id)
            .where(Author.author == author)
        ).all()
        papers = session.scalars(
            select(Paper).where(Paper.id.in_(paper_ids))
        ).all()
        return [PaperRepository._to_summary(session, p) for p in papers]

    @staticmethod
    def add_paper(session: Session, data: PaperCreate) -> Paper:
        """Insert a new paper with its BibTeX entry and author links.

        :param session: active SQLAlchemy session.
        :param data: :class:`PaperCreate` DTO with all required fields.
        :returns: the newly created :class:`Paper` ORM row.
        :raises IntegrityError: if ``bibtex_id`` already exists in ``bib``.
        """
        BibRepository.add_bib(session, data.bibtex_id, data.bibtex)
        paper = Paper(title=data.title, contents=data.contents, bibtex_id=data.bibtex_id)
        session.add(paper)
        session.flush()
        for name in data.authors:
            author_id = AuthorRepository.get_or_create_author(session, name)
            AuthorRepository.link_author_to_paper(session, author_id, paper.id)
        return paper

    @staticmethod
    def get_by_bibtex_id(session: Session, bibtex_id: str) -> PaperSummary | None:
        """Fetch a single paper by its BibTeX cite key.

        :param session: active SQLAlchemy session.
        :param bibtex_id: BibTeX cite key.
        :returns: :class:`PaperSummary`, or ``None`` if not found.
        """
        paper = session.scalar(select(Paper).where(Paper.bibtex_id == bibtex_id))
        if paper is None:
            return None
        return PaperRepository._to_summary(session, paper)

    @staticmethod
    def delete_paper(session: Session, bibtex_id: str) -> None:
        """Delete a paper and all associated data.

        Removes: ``authors_papers`` links, orphaned ``authors_id`` rows,
        the ``papers`` row, and the ``bib`` row.

        :param session: active SQLAlchemy session.
        :param bibtex_id: BibTeX cite key of the paper to delete.
        :raises ValueError: if no paper with *bibtex_id* exists.
        """
        paper = session.scalar(select(Paper).where(Paper.bibtex_id == bibtex_id))
        if paper is None:
            raise ValueError(f"Paper with BibTeX key '{bibtex_id}' not found.")
        paper_id: int = paper.id
        AuthorRepository.delete_links_for_paper(session, paper_id)
        session.delete(paper)
        session.flush()
        BibRepository.delete_bib(session, bibtex_id)

    @staticmethod
    def update_paper_field(
        session: Session, paper_id: int, column: str, value: str
    ) -> None:
        """Update a single column of a ``papers`` row.

        :param session: active SQLAlchemy session.
        :param paper_id: integer ID of the paper to update.
        :param column: column name — one of ``"title"`` or ``"contents"``.
        :param value: new value to set.
        :raises ValueError: if *column* is not an editable ``papers`` column.
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
                raise ValueError(f"Column '{column}' is not editable in the papers table.")
        session.flush()

    @staticmethod
    def _to_summary(session: Session, paper: Paper) -> PaperSummary:
        """Convert a :class:`Paper` ORM row to a :class:`PaperSummary` DTO.

        :param session: active SQLAlchemy session (used to fetch bib + authors).
        :param paper: ORM paper row.
        :returns: fully populated :class:`PaperSummary`.
        """
        bib = BibRepository.get_bib(session, paper.bibtex_id or "")
        authors = AuthorRepository.get_authors_for_paper(session, paper.id or 0)
        return PaperSummary(
            id=paper.id or 0,
            title=paper.title or "",
            contents=paper.contents or "",
            bibtex_id=paper.bibtex_id or "",
            bibtex=bib.bibtex if bib else "",
            authors=authors,
        )
