"""Repositories and DTOs (persistence layer).

The pydantic DTOs defined here are the *only* types that cross the ``db/``
boundary. Services consume ``PaperSummary`` / ``PaperCreate`` and never touch ORM
types, so the ORM/driver can be swapped as a single-package change (constitution
Principle I).
"""

from __future__ import annotations

from pydantic import BaseModel
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from paper_sorts.db.models import Author, AuthorPaper, Bib, Paper


class PaperSummary(BaseModel):
    """A fully-resolved paper record for display."""

    paper_id: int
    title: str
    authors: list[str]
    summary: str
    bibtex_id: str
    bibtex: str


class PaperCreate(BaseModel):
    """The input unit for adding a paper (single add or bulk import)."""

    title: str
    summary: str
    bibtex_id: str
    bibtex: str
    authors: list[str]


def _authors_for(session: Session, paper_id: int) -> list[str]:
    """Return the author names linked to ``paper_id`` in link order."""
    stmt = (
        select(Author.author)
        .join(AuthorPaper, AuthorPaper.author_id == Author.id)
        .where(AuthorPaper.paper_id == paper_id)
        .order_by(AuthorPaper.id)
    )
    return [a for a in session.scalars(stmt).all() if a is not None]


def _summary_for(session: Session, paper: Paper) -> PaperSummary:
    """Build a :class:`PaperSummary` from a paper row and its related data."""
    bibtex = ""
    if paper.bibtex_id is not None:
        bib_row = session.get(Bib, paper.bibtex_id)
        bibtex = bib_row.bibtex if bib_row is not None else ""
    return PaperSummary(
        paper_id=paper.id,
        title=paper.title or "",
        authors=_authors_for(session, paper.id),
        summary=paper.contents or "",
        bibtex_id=paper.bibtex_id or "",
        bibtex=bibtex or "",
    )


class PaperRepository:
    """Persistence operations for papers."""

    def get_by_title(self, session: Session, title: str) -> list[PaperSummary]:
        """Return all papers whose title exactly matches ``title``.

        :param session: an open session.
        :param title: the exact title to match.
        :returns: a list of resolved summaries (possibly empty).
        """
        papers = session.scalars(select(Paper).where(Paper.title == title)).all()
        return [_summary_for(session, p) for p in papers]

    def get_by_id(self, session: Session, paper_id: int) -> PaperSummary | None:
        """Return the paper with the given id, or ``None``."""
        paper = session.get(Paper, paper_id)
        return _summary_for(session, paper) if paper is not None else None

    def exists_bibtex_id(self, session: Session, bibtex_id: str) -> bool:
        """Return whether a paper already uses ``bibtex_id``."""
        stmt = select(func.count()).select_from(Paper).where(Paper.bibtex_id == bibtex_id)
        return bool(session.scalar(stmt))

    def add(self, session: Session, paper: PaperCreate) -> int:
        """Insert a paper row and return its new id.

        :param session: an open session.
        :param paper: the paper to insert (bib row must already exist).
        :returns: the new ``papers.id``.
        """
        row = Paper(title=paper.title, contents=paper.summary, bibtex_id=paper.bibtex_id)
        session.add(row)
        session.flush()
        return row.id

    def update_title(self, session: Session, paper_id: int, value: str) -> None:
        """Set the title of ``paper_id``."""
        paper = session.get(Paper, paper_id)
        if paper is None:
            raise ValueError(f"No paper with id {paper_id}")
        paper.title = value

    def update_contents(self, session: Session, paper_id: int, value: str) -> None:
        """Set the summary (``contents``) of ``paper_id``."""
        paper = session.get(Paper, paper_id)
        if paper is None:
            raise ValueError(f"No paper with id {paper_id}")
        paper.contents = value

    def delete(self, session: Session, paper_id: int) -> None:
        """Delete the paper row ``paper_id`` (links/bib handled by the service)."""
        session.execute(delete(Paper).where(Paper.id == paper_id))


class AuthorRepository:
    """Persistence operations for authors and the link table."""

    def get_papers_by_author(self, session: Session, author: str) -> list[PaperSummary]:
        """Return all papers linked to the author named ``author``."""
        stmt = (
            select(Paper)
            .join(AuthorPaper, AuthorPaper.paper_id == Paper.id)
            .join(Author, Author.id == AuthorPaper.author_id)
            .where(Author.author == author)
            .order_by(Paper.id)
        )
        return [_summary_for(session, p) for p in session.scalars(stmt).all()]

    def _author_id(self, session: Session, author: str) -> int | None:
        return session.scalar(select(Author.id).where(Author.author == author))

    def rename(self, session: Session, author_id: int, value: str) -> None:
        """Rename the author row ``author_id`` to ``value``."""
        author = session.get(Author, author_id)
        if author is None:
            raise ValueError(f"No author with id {author_id}")
        author.author = value

    def link(self, session: Session, author: str, paper_id: int) -> None:
        """Link ``author`` to ``paper_id``, creating the author row if absent."""
        author_id = self._author_id(session, author)
        if author_id is None:
            row = Author(author=author)
            session.add(row)
            session.flush()
            author_id = row.id
        session.add(AuthorPaper(author_id=author_id, paper_id=paper_id))

    def unlink_all_for_paper(self, session: Session, paper_id: int) -> None:
        """Remove every author link for ``paper_id``, deleting orphan authors."""
        author_ids = list(
            session.scalars(
                select(AuthorPaper.author_id).where(AuthorPaper.paper_id == paper_id)
            ).all()
        )
        session.execute(delete(AuthorPaper).where(AuthorPaper.paper_id == paper_id))
        session.flush()
        for author_id in author_ids:
            if author_id is None:
                continue
            remaining = session.scalar(
                select(func.count())
                .select_from(AuthorPaper)
                .where(AuthorPaper.author_id == author_id)
            )
            if not remaining:
                session.execute(delete(Author).where(Author.id == author_id))


class BibRepository:
    """Persistence operations for BibTeX entries."""

    def exists(self, session: Session, bibtex_id: str) -> bool:
        """Return whether a bib row with ``bibtex_id`` exists."""
        return session.get(Bib, bibtex_id) is not None

    def add(self, session: Session, bibtex_id: str, bibtex: str) -> None:
        """Insert a bib row."""
        session.add(Bib(bibtex_id=bibtex_id, bibtex=bibtex))

    def update(self, session: Session, bibtex_id: str, bibtex: str) -> None:
        """Replace the bibtex source for ``bibtex_id``; reject a duplicate value."""
        clash = session.scalar(
            select(Bib.bibtex_id).where(Bib.bibtex == bibtex, Bib.bibtex_id != bibtex_id)
        )
        if clash is not None:
            raise ValueError("bibtex is unique - value already exists!")
        row = session.get(Bib, bibtex_id)
        if row is None:
            raise ValueError(f"No bib entry with key {bibtex_id}")
        row.bibtex = bibtex

    def delete(self, session: Session, bibtex_id: str) -> None:
        """Delete the bib row ``bibtex_id``."""
        session.execute(delete(Bib).where(Bib.bibtex_id == bibtex_id))
