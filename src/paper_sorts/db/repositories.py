"""Repository classes and data-transfer objects for the persistence layer.

Repositories return pydantic DTOs (:class:`PaperSummary`, :class:`PaperCreate`),
never ORM instances, so the service layer never depends on SQLAlchemy types.
Only this package (``paper_sorts.db``) imports ``sqlalchemy``.
"""

from __future__ import annotations

from pydantic import BaseModel
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from paper_sorts.db.models import Author, AuthorPaper, Bib, Paper


class PaperSummary(BaseModel):
    """Read model for a paper, matching the legacy pretty-print shape.

    :ivar paper_id: internal ``papers.id``.
    :ivar title: publication title.
    :ivar authors: ``" and "``-joined ``"Last, First"`` author names.
    :ivar bibtex_id: BibTeX key.
    :ivar contents: one-sentence summary.
    :ivar bibtex: full BibTeX source string.
    """

    paper_id: int
    title: str
    authors: str
    bibtex_id: str
    contents: str
    bibtex: str


class PaperCreate(BaseModel):
    """Write model for adding a paper.

    :ivar title: publication title.
    :ivar contents: one-sentence summary.
    :ivar bibtex_id: BibTeX key (unique).
    :ivar bibtex: full BibTeX source string.
    :ivar authors: list of ``"Last, First"`` author names.
    """

    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: list[str]


class DuplicateBibtexError(Exception):
    """Raised when a BibTeX key or BibTeX source already exists."""


class PaperNotFoundError(Exception):
    """Raised when a paper cannot be located by the given identifier."""


def _join_authors(names: list[str]) -> str:
    """Join author names the way the legacy pretty-print did.

    :param names: ordered author names.
    :return: names joined by ``" and "``.
    """
    return " and ".join(names)


class PaperRepository:
    """Persistence operations for papers, their authors, and BibTeX entries."""

    def __init__(self, session: Session) -> None:
        """Bind the repository to an open session.

        :param session: an open SQLAlchemy session.
        """
        self._session = session

    def _summaries_for(self, paper_ids: list[int]) -> list[PaperSummary]:
        """Build :class:`PaperSummary` rows for the given paper ids, in order.

        :param paper_ids: paper ids to summarise.
        :return: one summary per paper id that still exists.
        """
        summaries: list[PaperSummary] = []
        for paper_id in paper_ids:
            paper = self._session.get(Paper, paper_id)
            if paper is None:
                continue
            author_rows = self._session.execute(
                select(Author.author)
                .join(AuthorPaper, AuthorPaper.author_id == Author.id)
                .where(AuthorPaper.paper_id == paper_id)
                .order_by(AuthorPaper.id)
            ).all()
            authors = _join_authors([row[0] for row in author_rows if row[0] is not None])
            bib = self._session.get(Bib, paper.bibtex_id) if paper.bibtex_id else None
            summaries.append(
                PaperSummary(
                    paper_id=paper.id,
                    title=paper.title or "",
                    authors=authors,
                    bibtex_id=paper.bibtex_id or "",
                    contents=paper.contents or "",
                    bibtex=(bib.bibtex if bib and bib.bibtex else ""),
                )
            )
        return summaries

    def search_by_title(self, title: str) -> list[PaperSummary]:
        """Return every paper whose title matches exactly.

        :param title: exact paper title to search for.
        :return: matching paper summaries (possibly several sharing a title).
        """
        ids = [
            row[0]
            for row in self._session.execute(
                select(Paper.id).where(Paper.title == title).order_by(Paper.id)
            ).all()
        ]
        return self._summaries_for(ids)

    def search_by_author(self, author: str) -> list[PaperSummary]:
        """Return every paper credited to the given author.

        :param author: exact author name (``"Last, First"``).
        :return: matching paper summaries.
        """
        ids = [
            row[0]
            for row in self._session.execute(
                select(Paper.id)
                .join(AuthorPaper, AuthorPaper.paper_id == Paper.id)
                .join(Author, Author.id == AuthorPaper.author_id)
                .where(Author.author == author)
                .order_by(Paper.id)
            ).all()
        ]
        return self._summaries_for(ids)

    def _author_id(self, name: str) -> int:
        """Return the id of an author, creating the row if absent.

        :param name: author name.
        :return: the author's id.
        """
        existing = self._session.execute(
            select(Author.id).where(Author.author == name)
        ).first()
        if existing is not None:
            return int(existing[0])
        author = Author(author=name)
        self._session.add(author)
        self._session.flush()
        return author.id

    def add(self, paper: PaperCreate) -> None:
        """Persist a paper, its BibTeX entry, and its author links atomically.

        :param paper: the paper to add.
        :raises DuplicateBibtexError: if the BibTeX key or source already exists.
        """
        if self._session.get(Bib, paper.bibtex_id) is not None:
            raise DuplicateBibtexError(f"BibTeX key {paper.bibtex_id!r} already exists")
        dup_source = self._session.execute(
            select(Bib.bibtex_id).where(Bib.bibtex == paper.bibtex)
        ).first()
        if dup_source is not None:
            raise DuplicateBibtexError("BibTeX source already exists")

        self._session.add(Bib(bibtex_id=paper.bibtex_id, bibtex=paper.bibtex))
        new_paper = Paper(
            title=paper.title, contents=paper.contents, bibtex_id=paper.bibtex_id
        )
        self._session.add(new_paper)
        self._session.flush()
        for name in paper.authors:
            author_id = self._author_id(name)
            self._session.add(AuthorPaper(author_id=author_id, paper_id=new_paper.id))

    def update_title(self, paper_id: int, value: str) -> None:
        """Update a paper's title.

        :param paper_id: internal paper id.
        :param value: new title.
        :raises PaperNotFoundError: if the paper does not exist.
        """
        paper = self._session.get(Paper, paper_id)
        if paper is None:
            raise PaperNotFoundError(f"No paper with id {paper_id}")
        paper.title = value

    def update_contents(self, paper_id: int, value: str) -> None:
        """Update a paper's summary.

        :param paper_id: internal paper id.
        :param value: new summary.
        :raises PaperNotFoundError: if the paper does not exist.
        """
        paper = self._session.get(Paper, paper_id)
        if paper is None:
            raise PaperNotFoundError(f"No paper with id {paper_id}")
        paper.contents = value

    def delete(self, paper_id: int) -> None:
        """Delete a paper, its author links, orphaned authors, and bib row.

        :param paper_id: internal paper id.
        :raises PaperNotFoundError: if the paper does not exist.
        """
        paper = self._session.get(Paper, paper_id)
        if paper is None:
            raise PaperNotFoundError(f"No paper with id {paper_id}")
        author_ids = [
            row[0]
            for row in self._session.execute(
                select(AuthorPaper.author_id).where(AuthorPaper.paper_id == paper_id)
            ).all()
        ]
        self._session.execute(
            delete(AuthorPaper).where(AuthorPaper.paper_id == paper_id)
        )
        for author_id in author_ids:
            remaining = self._session.execute(
                select(func.count())
                .select_from(AuthorPaper)
                .where(AuthorPaper.author_id == author_id)
            ).scalar_one()
            if remaining == 0:
                self._session.execute(delete(Author).where(Author.id == author_id))
        bibtex_id = paper.bibtex_id
        self._session.delete(paper)
        self._session.flush()
        if bibtex_id is not None:
            self._session.execute(delete(Bib).where(Bib.bibtex_id == bibtex_id))


class AuthorRepository:
    """Persistence operations for authors."""

    def __init__(self, session: Session) -> None:
        """Bind the repository to an open session.

        :param session: an open SQLAlchemy session.
        """
        self._session = session

    def rename(self, author_id: int, new_name: str) -> None:
        """Rename an author by id.

        :param author_id: id of the author to rename.
        :param new_name: new ``"Last, First"`` name.
        :raises PaperNotFoundError: if the author does not exist.
        """
        author = self._session.get(Author, author_id)
        if author is None:
            raise PaperNotFoundError(f"No author with id {author_id}")
        author.author = new_name


class BibRepository:
    """Persistence operations for BibTeX entries."""

    def __init__(self, session: Session) -> None:
        """Bind the repository to an open session.

        :param session: an open SQLAlchemy session.
        """
        self._session = session

    def update_bibtex(self, bibtex_id: str, value: str) -> None:
        """Update the BibTeX source for an entry.

        :param bibtex_id: BibTeX key identifying the entry.
        :param value: new BibTeX source string.
        :raises PaperNotFoundError: if the entry does not exist.
        :raises DuplicateBibtexError: if the new source already exists elsewhere.
        """
        bib = self._session.get(Bib, bibtex_id)
        if bib is None:
            raise PaperNotFoundError(f"No bib entry with key {bibtex_id!r}")
        dup = self._session.execute(
            select(Bib.bibtex_id).where(Bib.bibtex == value, Bib.bibtex_id != bibtex_id)
        ).first()
        if dup is not None:
            raise DuplicateBibtexError("BibTeX source already exists")
        bib.bibtex = value
