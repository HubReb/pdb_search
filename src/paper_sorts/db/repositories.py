"""Persistence-layer repositories and the DTOs they exchange with services.

Repositories wrap a :class:`~sqlalchemy.orm.Session` and emit parameterised
queries and joins over the canonical four-table schema. They return pydantic
DTOs (:class:`PaperSummary`, :class:`PaperCreate`) — never ORM instances — so
that the domain (service) layer never depends on SQLAlchemy types
(Constitution Principle I).
"""

from __future__ import annotations

from pydantic import BaseModel
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from paper_sorts.db.models import AuthorId, AuthorPaper, Bib, Paper


class PaperSummary(BaseModel):
    """Read model for a single paper hit.

    :ivar title: paper title.
    :ivar authors: author names in ``"Last, First"`` form, in stored order.
    :ivar summary: the paper's one-sentence summary (``papers.contents``).
    :ivar bibtex_id: the BibTeX key.
    :ivar bibtex: the full BibTeX source string.
    """

    title: str
    authors: list[str]
    summary: str
    bibtex_id: str
    bibtex: str


class PaperCreate(BaseModel):
    """Write model for adding a paper.

    :ivar title: paper title.
    :ivar authors: author names in ``"Last, First"`` form.
    :ivar summary: one-sentence summary.
    :ivar bibtex_id: the (unique) BibTeX key.
    :ivar bibtex: the full BibTeX source string.
    """

    title: str
    authors: list[str]
    summary: str
    bibtex_id: str
    bibtex: str


class DuplicateBibtexKeyError(Exception):
    """Raised when adding a paper whose BibTeX key already exists."""


class PaperNotFoundError(Exception):
    """Raised when an expected paper cannot be located."""


def _summary_from_rows(paper: Paper, authors: list[str], bibtex: str) -> PaperSummary:
    """Assemble a :class:`PaperSummary` from a paper row plus its authors/bib."""
    return PaperSummary(
        title=paper.title or "",
        authors=authors,
        summary=paper.contents or "",
        bibtex_id=paper.bibtex_id or "",
        bibtex=bibtex,
    )


class BibRepository:
    """Read/write access to the ``bib`` table."""

    def __init__(self, session: Session) -> None:
        """:param session: an open SQLAlchemy session."""
        self._session = session

    def exists(self, bibtex_id: str) -> bool:
        """Return whether a BibTeX entry with this key exists.

        :param bibtex_id: the BibTeX key to look up.
        :return: ``True`` if present.
        """
        return self._session.get(Bib, bibtex_id) is not None

    def get(self, bibtex_id: str) -> str | None:
        """Return the BibTeX source for a key, or ``None`` if absent.

        :param bibtex_id: the BibTeX key.
        :return: the BibTeX source string, or ``None``.
        """
        bib = self._session.get(Bib, bibtex_id)
        return None if bib is None else (bib.bibtex or "")

    def update_bibtex(self, bibtex_id: str, new_bibtex: str) -> None:
        """Update the BibTeX source for a key.

        :param bibtex_id: the BibTeX key.
        :param new_bibtex: the new BibTeX source.
        :raises PaperNotFoundError: if the key is absent.
        :raises DuplicateBibtexKeyError: if ``new_bibtex`` already exists elsewhere.
        """
        existing = self._session.scalar(select(Bib).where(Bib.bibtex == new_bibtex))
        if existing is not None and existing.bibtex_id != bibtex_id:
            raise DuplicateBibtexKeyError("bibtex is unique — value already exists")
        bib = self._session.get(Bib, bibtex_id)
        if bib is None:
            raise PaperNotFoundError(f"bibtex_id {bibtex_id!r} not found")
        bib.bibtex = new_bibtex


class AuthorRepository:
    """Read/write access to ``authors_id`` and ``authors_papers``."""

    def __init__(self, session: Session) -> None:
        """:param session: an open SQLAlchemy session."""
        self._session = session

    def get_or_create(self, name: str) -> int:
        """Return the id of an author, creating the row if absent.

        :param name: the author name in ``"Last, First"`` form.
        :return: the ``authors_id.id`` of the (possibly new) author.
        """
        existing = self._session.scalar(select(AuthorId).where(AuthorId.author == name))
        if existing is not None:
            return existing.id
        author = AuthorId(author=name)
        self._session.add(author)
        self._session.flush()
        return author.id

    def link(self, author_id: int, paper_id: int) -> None:
        """Link an author to a paper in ``authors_papers``.

        :param author_id: the author id.
        :param paper_id: the paper id.
        """
        self._session.add(AuthorPaper(author_id=author_id, paper_id=paper_id))

    def names_for_paper(self, paper_id: int) -> list[str]:
        """Return the author names linked to a paper, in stored order.

        :param paper_id: the paper id.
        :return: a list of author names.
        """
        rows = self._session.execute(
            select(AuthorId.author)
            .join(AuthorPaper, AuthorPaper.author_id == AuthorId.id)
            .where(AuthorPaper.paper_id == paper_id)
            .order_by(AuthorPaper.id)
        ).all()
        return [row[0] or "" for row in rows]

    def rename(self, old_name: str, new_name: str) -> None:
        """Rename an author, merging onto an existing target and cleaning orphans.

        If ``new_name`` already exists, the links of ``old_name`` are repointed to
        it and duplicate links removed; otherwise ``old_name`` is renamed in place.
        An author left with no papers is deleted.

        :param old_name: the current author name.
        :param new_name: the desired author name.
        :raises PaperNotFoundError: if ``old_name`` does not exist.
        """
        old = self._session.scalar(select(AuthorId).where(AuthorId.author == old_name))
        if old is None:
            raise PaperNotFoundError(f"author {old_name!r} not found")
        target = self._session.scalar(select(AuthorId).where(AuthorId.author == new_name))
        if target is None:
            old.author = new_name
            return
        # Repoint links from old to target, then dedupe and clean up.
        links = self._session.scalars(
            select(AuthorPaper).where(AuthorPaper.author_id == old.id)
        ).all()
        target_papers = {
            ap.paper_id
            for ap in self._session.scalars(
                select(AuthorPaper).where(AuthorPaper.author_id == target.id)
            ).all()
        }
        for link in links:
            if link.paper_id in target_papers:
                self._session.delete(link)
            else:
                link.author_id = target.id
                target_papers.add(link.paper_id)
        self._session.delete(old)

    def delete_link_and_cleanup(self, author_id: int, paper_id: int) -> None:
        """Remove an author-paper link and delete the author if now orphaned.

        :param author_id: the author id.
        :param paper_id: the paper id.
        """
        self._session.execute(
            delete(AuthorPaper).where(
                AuthorPaper.author_id == author_id, AuthorPaper.paper_id == paper_id
            )
        )
        remaining = self._session.scalar(
            select(func.count()).select_from(AuthorPaper).where(AuthorPaper.author_id == author_id)
        )
        if not remaining:
            self._session.execute(delete(AuthorId).where(AuthorId.id == author_id))


class PaperRepository:
    """Read/write access to ``papers``, joined with authors and bib."""

    def __init__(self, session: Session) -> None:
        """:param session: an open SQLAlchemy session."""
        self._session = session
        self._authors = AuthorRepository(session)
        self._bib = BibRepository(session)

    def search_by_title(self, title: str) -> list[PaperSummary]:
        """Return all papers whose title matches exactly.

        :param title: the title to search for.
        :return: a list of :class:`PaperSummary`, one per matching paper.
        """
        papers = self._session.scalars(select(Paper).where(Paper.title == title)).all()
        return [self._to_summary(p) for p in papers]

    def search_by_author(self, author: str) -> list[PaperSummary]:
        """Return all papers an author is credited on.

        :param author: the author name to search for.
        :return: a list of :class:`PaperSummary`, one per paper.
        """
        papers = self._session.scalars(
            select(Paper)
            .join(AuthorPaper, AuthorPaper.paper_id == Paper.id)
            .join(AuthorId, AuthorId.id == AuthorPaper.author_id)
            .where(AuthorId.author == author)
            .order_by(Paper.id)
        ).all()
        return [self._to_summary(p) for p in papers]

    def _to_summary(self, paper: Paper) -> PaperSummary:
        """Build a :class:`PaperSummary` for a paper row."""
        authors = self._authors.names_for_paper(paper.id)
        bibtex = self._bib.get(paper.bibtex_id or "") or ""
        return _summary_from_rows(paper, authors, bibtex)

    def add(self, paper: PaperCreate) -> int:
        """Insert a paper, its bib entry, authors, and authorship links.

        :param paper: the paper to add.
        :return: the new ``papers.id``.
        :raises DuplicateBibtexKeyError: if the BibTeX key already exists.
        """
        if self._bib.exists(paper.bibtex_id):
            raise DuplicateBibtexKeyError(f"bibtex key {paper.bibtex_id!r} already exists")
        self._session.add(Bib(bibtex_id=paper.bibtex_id, bibtex=paper.bibtex))
        row = Paper(title=paper.title, contents=paper.summary, bibtex_id=paper.bibtex_id)
        self._session.add(row)
        self._session.flush()
        for name in paper.authors:
            author_id = self._authors.get_or_create(name)
            self._authors.link(author_id, row.id)
        return row.id

    def update_title(self, paper_id: int, new_title: str) -> None:
        """Update a paper's title.

        :param paper_id: the paper id.
        :param new_title: the new title.
        :raises PaperNotFoundError: if the paper does not exist.
        """
        paper = self._require(paper_id)
        paper.title = new_title

    def update_contents(self, paper_id: int, new_contents: str) -> None:
        """Update a paper's summary (``contents``).

        :param paper_id: the paper id.
        :param new_contents: the new summary.
        :raises PaperNotFoundError: if the paper does not exist.
        """
        paper = self._require(paper_id)
        paper.contents = new_contents

    def _require(self, paper_id: int) -> Paper:
        """Return the paper or raise :class:`PaperNotFoundError`."""
        paper = self._session.get(Paper, paper_id)
        if paper is None:
            raise PaperNotFoundError(f"paper id {paper_id!r} not found")
        return paper

    def delete(self, summary: PaperSummary) -> None:
        """Delete a paper, its authorship links, orphaned authors, and bib row.

        :param summary: the paper to delete (located by title).
        :raises PaperNotFoundError: if the paper does not exist.
        """
        paper = self._session.scalar(select(Paper).where(Paper.title == summary.title))
        if paper is None:
            raise PaperNotFoundError(f"paper {summary.title!r} not found")
        for name in summary.authors:
            author = self._session.scalar(select(AuthorId).where(AuthorId.author == name))
            if author is not None:
                self._authors.delete_link_and_cleanup(author.id, paper.id)
        self._session.execute(delete(Paper).where(Paper.id == paper.id))
        self._session.execute(delete(Bib).where(Bib.bibtex_id == summary.bibtex_id))
