"""Repositories and DTOs for the persistence layer.

Repositories accept an open :class:`~sqlalchemy.orm.Session` and expose pydantic DTOs
(:class:`PaperSummary`, :class:`PaperCreate`) rather than ORM types, so the service layer never
imports ``sqlalchemy``. All queries are parameterised and use joins over the existing four-table
schema (no new tables or indexes).

The author/link semantics mirror the legacy tool: authors are stored once in ``authors_id`` and
linked through ``authors_papers``; when a paper is deleted, authors left with no remaining
papers are removed (orphan cleanup). Two authors with an identical ``"Last, First"`` string are
treated as the same author — a preserved limitation.
"""

from __future__ import annotations

from pydantic import BaseModel
from sqlalchemy import delete, func, select, update
from sqlalchemy.orm import Session

from paper_sorts.db.models import AuthorId, AuthorPaper, Bib, Paper


class PaperSummary(BaseModel):
    """A read model for one paper, matching the legacy pretty-print record.

    :ivar paper_id: internal ``papers.id``.
    :ivar title: the paper title.
    :ivar authors: the paper's authors joined with ``" and "``.
    :ivar summary: the paper summary (``papers.contents``).
    :ivar bibtex_id: the user-facing BibTeX key.
    :ivar bibtex: the full BibTeX source string.
    """

    paper_id: int
    title: str
    authors: str
    summary: str
    bibtex_id: str
    bibtex: str


class PaperCreate(BaseModel):
    """A write model for adding one paper.

    :ivar title: the paper title.
    :ivar summary: the paper summary (stored in ``papers.contents``).
    :ivar bibtex_id: the unique BibTeX key.
    :ivar bibtex: the full BibTeX source string.
    :ivar authors: one ``"Last, First"`` string per author.
    """

    title: str
    summary: str
    bibtex_id: str
    bibtex: str
    authors: list[str]


class BibRepository:
    """Persistence operations for the ``bib`` table."""

    def __init__(self, session: Session) -> None:
        """Bind the repository to an open session.

        :param session: an open SQLAlchemy session.
        """
        self.session = session

    def exists(self, bibtex_id: str) -> bool:
        """Return whether a BibTeX entry with the given key exists.

        :param bibtex_id: the BibTeX key to check.
        :return: ``True`` if present, else ``False``.
        """
        return (
            self.session.execute(
                select(Bib.bibtex_id).where(Bib.bibtex_id == bibtex_id)
            ).first()
            is not None
        )

    def add(self, bibtex_id: str, bibtex: str) -> None:
        """Insert a BibTeX entry.

        :param bibtex_id: the unique BibTeX key.
        :param bibtex: the full BibTeX source string.
        """
        self.session.add(Bib(bibtex_id=bibtex_id, bibtex=bibtex))
        self.session.flush()

    def update_bibtex(self, bibtex_id: str, new_bibtex: str) -> None:
        """Replace the BibTeX source for an existing key.

        :param bibtex_id: the BibTeX key whose source is replaced.
        :param new_bibtex: the new BibTeX source string.
        :raises ValueError: if the new source already exists (UNIQUE constraint).
        """
        clash = self.session.execute(
            select(Bib.bibtex_id).where(Bib.bibtex == new_bibtex)
        ).first()
        if clash is not None:
            raise ValueError("bibtex is unique - value already exists!")
        bib = self.session.get(Bib, bibtex_id)
        if bib is None:
            raise ValueError(f"No bib entry with key {bibtex_id}")
        bib.bibtex = new_bibtex
        self.session.flush()

    def delete(self, bibtex_id: str) -> None:
        """Delete a BibTeX entry by key.

        :param bibtex_id: the BibTeX key to delete.
        """
        self.session.execute(delete(Bib).where(Bib.bibtex_id == bibtex_id))
        self.session.flush()


class AuthorRepository:
    """Persistence operations for ``authors_id`` and ``authors_papers``."""

    def __init__(self, session: Session) -> None:
        """Bind the repository to an open session.

        :param session: an open SQLAlchemy session.
        """
        self.session = session

    def get_or_create_author_id(self, author: str) -> int:
        """Return the id of the author, creating the row if needed.

        :param author: the author name in ``"Last, First"`` form.
        :return: the ``authors_id.id`` of the author.
        """
        existing = self.session.execute(
            select(AuthorId.id).where(AuthorId.author == author)
        ).scalar_one_or_none()
        if existing is not None:
            return existing
        row = AuthorId(author=author)
        self.session.add(row)
        self.session.flush()
        assert row.id is not None
        return row.id

    def link(self, author_id: int, paper_id: int) -> None:
        """Create an author-paper link.

        :param author_id: the ``authors_id.id``.
        :param paper_id: the ``papers.id``.
        """
        self.session.add(AuthorPaper(author_id=author_id, paper_id=paper_id))
        self.session.flush()

    def update_author_name(self, old_name: str, new_name: str) -> None:
        """Rename an author, re-pointing links and cleaning up orphans.

        If ``new_name`` already exists, the old author's links are re-pointed to the existing
        author (de-duplicating); otherwise the existing row is simply renamed. The old author
        row is removed if it ends up with no papers.

        :param old_name: the current author name.
        :param new_name: the desired author name.
        :raises ValueError: if no author named ``old_name`` exists.
        """
        old_id = self.session.execute(
            select(AuthorId.id).where(AuthorId.author == old_name)
        ).scalar_one_or_none()
        if old_id is None:
            raise ValueError(f"No author named {old_name}")
        new_id = self.session.execute(
            select(AuthorId.id).where(AuthorId.author == new_name)
        ).scalar_one_or_none()
        if new_id is None:
            old_row = self.session.get(AuthorId, old_id)
            assert old_row is not None
            old_row.author = new_name
            self.session.flush()
            return
        # Re-point links from the old author to the existing new author, dropping duplicates.
        self.session.execute(
            update(AuthorPaper).where(AuthorPaper.author_id == old_id).values(author_id=new_id)
        )
        self._dedupe_links(new_id)
        self.session.execute(delete(AuthorId).where(AuthorId.id == old_id))
        self.session.flush()

    def _dedupe_links(self, author_id: int) -> None:
        """Drop duplicate (author_id, paper_id) links for an author, keeping the lowest id.

        :param author_id: the author whose links are de-duplicated.
        """
        rows = self.session.execute(
            select(AuthorPaper.id, AuthorPaper.paper_id).where(
                AuthorPaper.author_id == author_id
            )
        ).all()
        seen: set[int | None] = set()
        for link_id, paper_id in rows:
            if paper_id in seen:
                self.session.execute(delete(AuthorPaper).where(AuthorPaper.id == link_id))
            else:
                seen.add(paper_id)
        self.session.flush()

    def unlink_for_paper(self, author: str, paper_id: int) -> None:
        """Remove an author's link to a paper and delete the author if now orphaned.

        :param author: the author name.
        :param paper_id: the ``papers.id`` to unlink from.
        """
        author_id = self.session.execute(
            select(AuthorId.id).where(AuthorId.author == author)
        ).scalar_one_or_none()
        if author_id is None:
            return
        self.session.execute(
            delete(AuthorPaper).where(
                AuthorPaper.author_id == author_id, AuthorPaper.paper_id == paper_id
            )
        )
        self.session.flush()
        remaining = self.session.execute(
            select(AuthorPaper.id).where(AuthorPaper.author_id == author_id)
        ).first()
        if remaining is None:
            self.session.execute(delete(AuthorId).where(AuthorId.id == author_id))
            self.session.flush()


class PaperRepository:
    """Persistence operations for the ``papers`` table and its read models."""

    def __init__(self, session: Session) -> None:
        """Bind the repository to an open session.

        :param session: an open SQLAlchemy session.
        """
        self.session = session

    def _summaries_for(self, papers: list[Paper]) -> list[PaperSummary]:
        """Assemble :class:`PaperSummary` DTOs for the given papers.

        :param papers: ORM ``Paper`` rows.
        :return: a summary per paper, authors joined with ``" and "``.
        """
        summaries: list[PaperSummary] = []
        for paper in papers:
            assert paper.id is not None
            authors = self.session.execute(
                select(AuthorId.author)
                .join(AuthorPaper, AuthorPaper.author_id == AuthorId.id)
                .where(AuthorPaper.paper_id == paper.id)
            ).scalars().all()
            bibtex = ""
            if paper.bibtex_id is not None:
                bib = self.session.get(Bib, paper.bibtex_id)
                if bib is not None and bib.bibtex is not None:
                    bibtex = bib.bibtex
            summaries.append(
                PaperSummary(
                    paper_id=paper.id,
                    title=paper.title or "",
                    authors=" and ".join(a for a in authors if a is not None),
                    summary=paper.contents or "",
                    bibtex_id=paper.bibtex_id or "",
                    bibtex=bibtex,
                )
            )
        return summaries

    def search_by_title(self, title: str) -> list[PaperSummary]:
        """Return summaries for all papers whose title matches exactly.

        :param title: the exact paper title to search for.
        :return: a list of matching summaries (possibly empty).
        """
        papers = list(self.session.execute(select(Paper).where(Paper.title == title)).scalars())
        return self._summaries_for(papers)

    def search_by_author(self, author: str) -> list[PaperSummary]:
        """Return summaries for all papers linked to the given author.

        :param author: the exact author name (``"Last, First"``).
        :return: a list of matching summaries (possibly empty).
        """
        papers = list(
            self.session.execute(
                select(Paper)
                .join(AuthorPaper, AuthorPaper.paper_id == Paper.id)
                .join(AuthorId, AuthorId.id == AuthorPaper.author_id)
                .where(AuthorId.author == author)
            ).scalars()
        )
        return self._summaries_for(papers)

    def get_by_id(self, paper_id: int) -> PaperSummary | None:
        """Return the summary for a single paper id.

        :param paper_id: the ``papers.id``.
        :return: the summary, or ``None`` if not found.
        """
        paper = self.session.get(Paper, paper_id)
        if paper is None:
            return None
        return self._summaries_for([paper])[0]

    def add_paper_row(self, title: str, contents: str, bibtex_id: str) -> int:
        """Insert a row into ``papers`` and return its id.

        :param title: the paper title.
        :param contents: the paper summary.
        :param bibtex_id: the BibTeX key (FK into ``bib``).
        :return: the new ``papers.id``.
        """
        row = Paper(title=title, contents=contents, bibtex_id=bibtex_id)
        self.session.add(row)
        self.session.flush()
        assert row.id is not None
        return row.id

    def update_papers_column(self, paper_id: int, column: str, value: str) -> None:
        """Update the ``title`` or ``contents`` column of a paper.

        :param paper_id: the ``papers.id`` to update.
        :param column: either ``"title"`` or ``"contents"``.
        :param value: the new value.
        :raises ValueError: if the column is not editable or the paper is missing.
        """
        if column not in {"title", "contents"}:
            raise ValueError(f"Column {column} is not present in table papers")
        paper = self.session.get(Paper, paper_id)
        if paper is None:
            raise ValueError(f"No paper with id {paper_id}")
        setattr(paper, column, value)
        self.session.flush()

    def delete_paper(self, paper_id: int) -> None:
        """Delete a paper row by id.

        :param paper_id: the ``papers.id`` to delete.
        """
        self.session.execute(delete(Paper).where(Paper.id == paper_id))
        self.session.flush()

    def count(self) -> int:
        """Return the number of papers.

        :return: the row count of ``papers``.
        """
        return int(self.session.execute(select(func.count()).select_from(Paper)).scalar_one())
