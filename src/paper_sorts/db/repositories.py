"""Repository classes and DTOs for the persistence layer.

Repositories are the only types the service layer may touch; ORM model
instances never leave this module. The pydantic DTOs :class:`PaperSummary` and
:class:`PaperCreate` cross the boundary instead, so an ORM or driver swap is a
single-package change (constitution Principle I).
"""

from __future__ import annotations

from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from paper_sorts.db.models import AuthorId, AuthorPaper, Bib, Paper


class PaperSummary(BaseModel):
    """A resolved paper for display, mirroring the legacy result shape.

    :ivar authors: ``" and "``-joined ``"Last, First"`` author names.
    :ivar paper_id: internal ``papers.id``.
    :ivar title: paper title.
    :ivar bibtex_id: the BibTeX key.
    :ivar contents: the paper summary.
    :ivar bibtex: the full BibTeX source, when resolved.
    """

    authors: str
    paper_id: int
    title: str
    bibtex_id: str
    contents: str
    bibtex: str | None = None


class PaperCreate(BaseModel):
    """A new paper to insert.

    :ivar title: paper title.
    :ivar contents: paper summary.
    :ivar bibtex_id: the unique BibTeX key.
    :ivar bibtex: the full BibTeX source string.
    :ivar authors: author names in ``"Last, First"`` form.
    """

    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: list[str]


class DuplicateError(ValueError):
    """Raised when a unique constraint (BibTeX key or string) would be violated."""


class NotFoundError(ValueError):
    """Raised when a referenced paper, author, or bib entry does not exist."""


class AuthorRepository:
    """Author and authorship operations over ``authors_id``/``authors_papers``."""

    def __init__(self, session: Session) -> None:
        """Bind the repository to a session.

        :param session: an open SQLAlchemy session.
        """
        self.session = session

    def link_author(self, name: str, paper_id: int) -> None:
        """Ensure an author exists and link them to a paper.

        :param name: the author's ``"Last, First"`` name.
        :param paper_id: the paper to link to.
        """
        author = self.session.scalars(select(AuthorId).where(AuthorId.author == name)).first()
        if author is None:
            author = AuthorId(author=name)
            self.session.add(author)
            self.session.flush()
        self.session.add(AuthorPaper(author_id=author.id, paper_id=paper_id))

    def unlink_authors_for_paper(self, author_names: list[str], paper_id: int) -> None:
        """Remove a paper's authorship links and drop newly-orphaned authors.

        :param author_names: the author names to unlink for this paper.
        :param paper_id: the paper whose links to remove.
        """
        for name in author_names:
            author = self.session.scalars(select(AuthorId).where(AuthorId.author == name)).first()
            if author is None:
                continue
            link = self.session.scalars(
                select(AuthorPaper).where(
                    AuthorPaper.author_id == author.id,
                    AuthorPaper.paper_id == paper_id,
                )
            ).first()
            if link is not None:
                self.session.delete(link)
                self.session.flush()
            self._drop_if_orphan(author.id)

    def rename_author(self, old_name: str, new_name: str) -> None:
        """Rename an author, merging onto an existing author if the name exists.

        :param old_name: the current author name to change.
        :param new_name: the new author name.
        :raises NotFoundError: if no author has ``old_name``.
        """
        old = self.session.scalars(select(AuthorId).where(AuthorId.author == old_name)).first()
        if old is None:
            raise NotFoundError(f"Author {old_name!r} not found")
        existing = self.session.scalars(select(AuthorId).where(AuthorId.author == new_name)).first()
        if existing is not None and existing.id != old.id:
            for link in self.session.scalars(
                select(AuthorPaper).where(AuthorPaper.author_id == old.id)
            ):
                link.author_id = existing.id
            self.session.flush()
            self.session.delete(old)
        else:
            old.author = new_name
        self.session.flush()

    def _drop_if_orphan(self, author_id: int) -> None:
        """Delete an author that no longer links to any paper.

        :param author_id: the author to check and possibly remove.
        """
        remaining = self.session.scalars(
            select(AuthorPaper).where(AuthorPaper.author_id == author_id)
        ).first()
        if remaining is None:
            author = self.session.get(AuthorId, author_id)
            if author is not None:
                self.session.delete(author)
                self.session.flush()


class BibRepository:
    """BibTeX-entry operations over the ``bib`` table."""

    def __init__(self, session: Session) -> None:
        """Bind the repository to a session.

        :param session: an open SQLAlchemy session.
        """
        self.session = session

    def update_bibtex(self, bibtex_id: str, new_bibtex: str) -> None:
        """Replace a bib entry's source string, enforcing uniqueness.

        :param bibtex_id: the BibTeX key whose entry to update.
        :param new_bibtex: the new source string.
        :raises DuplicateError: if another entry already has ``new_bibtex``.
        :raises NotFoundError: if no entry has ``bibtex_id``.
        """
        clash = self.session.scalars(select(Bib).where(Bib.bibtex == new_bibtex)).first()
        if clash is not None:
            raise DuplicateError("bibtex is unique - value already exists!")
        entry = self.session.get(Bib, bibtex_id)
        if entry is None:
            raise NotFoundError(f"BibTeX entry {bibtex_id!r} not found")
        entry.bibtex = new_bibtex


class PaperRepository:
    """Paper CRUD and search over the four-table schema via parameterised joins."""

    def __init__(self, session: Session) -> None:
        """Bind the repository to a session.

        :param session: an open SQLAlchemy session.
        """
        self.session = session
        self.authors = AuthorRepository(session)
        self.bib = BibRepository(session)

    def _summaries_for(self, papers: list[Paper]) -> list[PaperSummary]:
        """Build display summaries (with joined author strings) for papers.

        :param papers: the ORM papers to summarise.
        :returns: one :class:`PaperSummary` per paper.
        """
        summaries: list[PaperSummary] = []
        for paper in papers:
            names = (
                self.session.execute(
                    select(AuthorId.author)
                    .join(AuthorPaper, AuthorPaper.author_id == AuthorId.id)
                    .where(AuthorPaper.paper_id == paper.id)
                )
                .scalars()
                .all()
            )
            bib = self.session.get(Bib, paper.bibtex_id) if paper.bibtex_id else None
            summaries.append(
                PaperSummary(
                    authors=" and ".join(n for n in names if n),
                    paper_id=paper.id,
                    title=paper.title or "",
                    bibtex_id=paper.bibtex_id or "",
                    contents=paper.contents or "",
                    bibtex=bib.bibtex if bib is not None else None,
                )
            )
        return summaries

    def search_by_title(self, title: str) -> list[PaperSummary]:
        """Find papers whose title exactly matches ``title``.

        :param title: the title to search for.
        :returns: summaries for each matching paper (empty if none).
        """
        papers = list(self.session.scalars(select(Paper).where(Paper.title == title)).all())
        return self._summaries_for(papers)

    def search_by_author(self, author: str) -> list[PaperSummary]:
        """Find papers credited to ``author``.

        :param author: the ``"Last, First"`` author name to search for.
        :returns: summaries for each matching paper (empty if none).
        """
        papers = list(
            self.session.scalars(
                select(Paper)
                .join(AuthorPaper, AuthorPaper.paper_id == Paper.id)
                .join(AuthorId, AuthorId.id == AuthorPaper.author_id)
                .where(AuthorId.author == author)
            ).all()
        )
        return self._summaries_for(papers)

    def add_paper(self, data: PaperCreate) -> int:
        """Insert a paper, its bib entry, and its author links in one transaction.

        :param data: the paper to insert.
        :returns: the new ``papers.id``.
        :raises DuplicateError: if the BibTeX key already exists.
        """
        if self.session.get(Bib, data.bibtex_id) is not None:
            raise DuplicateError(f"Entry {data.bibtex_id!r} already exists")
        self.session.add(Bib(bibtex_id=data.bibtex_id, bibtex=data.bibtex))
        self.session.flush()
        paper = Paper(title=data.title, contents=data.contents, bibtex_id=data.bibtex_id)
        self.session.add(paper)
        self.session.flush()
        for name in data.authors:
            self.authors.link_author(name, paper.id)
        self.session.flush()
        return paper.id

    def delete_paper(self, bibtex_id: str) -> None:
        """Delete a paper, its bib entry, its links, and any orphaned authors.

        :param bibtex_id: the BibTeX key of the paper to delete.
        :raises NotFoundError: if no paper has ``bibtex_id``.
        """
        paper = self.session.scalars(select(Paper).where(Paper.bibtex_id == bibtex_id)).first()
        if paper is None:
            raise NotFoundError(f"Paper with bibtex {bibtex_id!r} not found")
        author_ids = (
            self.session.execute(
                select(AuthorPaper.author_id).where(AuthorPaper.paper_id == paper.id)
            )
            .scalars()
            .all()
        )
        for link in self.session.scalars(
            select(AuthorPaper).where(AuthorPaper.paper_id == paper.id)
        ):
            self.session.delete(link)
        self.session.flush()
        self.session.delete(paper)
        bib = self.session.get(Bib, bibtex_id)
        if bib is not None:
            self.session.delete(bib)
        self.session.flush()
        for author_id in author_ids:
            if author_id is not None:
                self.authors._drop_if_orphan(author_id)

    def update_paper_field(self, paper_id: int, column: str, value: str) -> None:
        """Update a paper's ``title`` or ``contents`` by id.

        :param paper_id: the paper to update.
        :param column: ``"title"`` or ``"contents"``.
        :param value: the new value.
        :raises NotFoundError: if no paper has ``paper_id``.
        :raises ValueError: if ``column`` is not updatable.
        """
        paper = self.session.get(Paper, paper_id)
        if paper is None:
            raise NotFoundError(f"Paper id {paper_id!r} not found")
        if column == "title":
            paper.title = value
        elif column == "contents":
            paper.contents = value
        else:
            raise ValueError(f"Column {column!r} is not present in table papers")
