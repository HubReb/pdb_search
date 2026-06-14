"""Repository classes and Pydantic DTOs for paper_sorts persistence layer.

This module is the sole interface between the service layer and the ORM.
Services pass in an open :class:`~sqlalchemy.orm.Session`; repositories
perform the query / mutation and return plain Pydantic DTOs — never ORM objects.

Only this module (and the rest of ``src/paper_sorts/db/``) may import
``sqlalchemy``.

Public symbols:
    :class:`PaperSummary` — read-only DTO returned from searches.
    :class:`PaperCreate` — input DTO for adding a new paper.
    :class:`PaperRepository` — CRUD + search operations on papers.
"""

from __future__ import annotations

import logging
from typing import Literal

from pydantic import BaseModel
from sqlalchemy import delete, select, update
from sqlalchemy.orm import Session

from paper_sorts.db.models import Author, AuthorPaper, Bib, Paper

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DTOs
# ---------------------------------------------------------------------------


class PaperSummary(BaseModel):
    """Read-only view of a paper returned from search operations.

    :param id: Internal paper ID from the ``papers`` table.
    :param title: Publication title.
    :param contents: Short summary / abstract.
    :param bibtex_id: BibTeX citation key (the user-facing unique identifier).
    :param authors: List of author name strings in ``"Last, First"`` form.
    :param bibtex: Full BibTeX source string.
    """

    id: int
    title: str
    contents: str
    bibtex_id: str
    authors: list[str]
    bibtex: str


class PaperCreate(BaseModel):
    """Input DTO for creating a new paper.

    :param title: Publication title.
    :param contents: Short summary / abstract.
    :param bibtex_id: BibTeX citation key (must be unique).
    :param bibtex: Full BibTeX source string.
    :param authors: List of author names in ``"Last, First"`` form.
    """

    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: list[str]


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------


class PaperRepository:
    """Repository for paper CRUD and search operations.

    All methods accept an open :class:`~sqlalchemy.orm.Session` as their first
    argument.  Callers are responsible for committing or rolling back; the
    repository does not call ``session.commit()``.
    """

    def _build_summary(self, paper: Paper, session: Session) -> PaperSummary:
        """Build a :class:`PaperSummary` from an ORM Paper object.

        :param paper: ORM paper instance (must be attached to session).
        :param session: Active SQLAlchemy session.
        :return: Populated :class:`PaperSummary` DTO.
        """
        author_rows = session.execute(
            select(Author.author)
            .join(AuthorPaper, AuthorPaper.author_id == Author.id)
            .where(AuthorPaper.paper_id == paper.id)
        ).scalars().all()

        bib_row = session.execute(
            select(Bib.bibtex).where(Bib.bibtex_id == paper.bibtex_id)
        ).scalar_one_or_none()

        return PaperSummary(
            id=paper.id,
            title=paper.title or "",
            contents=paper.contents or "",
            bibtex_id=paper.bibtex_id or "",
            authors=[a for a in author_rows if a is not None],
            bibtex=bib_row or "",
        )

    def search_by_title(self, session: Session, title: str) -> list[PaperSummary]:
        """Search for papers whose title exactly matches *title*.

        :param session: Active SQLAlchemy session.
        :param title: Title string to match (case-sensitive equality).
        :return: List of matching :class:`PaperSummary` DTOs (empty if none found).
        """
        papers = session.execute(
            select(Paper).where(Paper.title == title)
        ).scalars().all()
        results = [self._build_summary(p, session) for p in papers]
        if not results:
            logger.info("Paper with title %r not found", title)
        return results

    def search_by_author(self, session: Session, author: str) -> list[PaperSummary]:
        """Search for papers by author name.

        :param session: Active SQLAlchemy session.
        :param author: Author name string (case-sensitive equality).
        :return: List of :class:`PaperSummary` DTOs for papers authored by *author*.
        """
        paper_ids = session.execute(
            select(AuthorPaper.paper_id)
            .join(Author, Author.id == AuthorPaper.author_id)
            .where(Author.author == author)
        ).scalars().all()

        results: list[PaperSummary] = []
        seen: set[int] = set()
        for pid in paper_ids:
            if pid is None:
                continue
            if pid in seen:
                continue
            seen.add(pid)
            paper = session.get(Paper, pid)
            if paper is not None:
                results.append(self._build_summary(paper, session))
        if not results:
            logger.info("Author %r not found", author)
        return results

    def add_paper(self, session: Session, paper: PaperCreate) -> PaperSummary:
        """Add a new paper to the database within the given session.

        Inserts a :class:`~paper_sorts.db.models.Bib` row, a
        :class:`~paper_sorts.db.models.Paper` row, and
        :class:`~paper_sorts.db.models.Author` /
        :class:`~paper_sorts.db.models.AuthorPaper` rows for each author.
        Author rows are reused if the name already exists.

        Raises an ``IntegrityError`` (propagated from SQLAlchemy) if
        ``bibtex_id`` or ``bibtex`` are not unique.

        :param session: Active SQLAlchemy session (caller commits or rolls back).
        :param paper: :class:`PaperCreate` DTO with the paper data.
        :return: :class:`PaperSummary` of the newly created paper.
        :raises sqlalchemy.exc.IntegrityError: If bibtex_id or bibtex already exist.
        """
        bib_row = Bib(bibtex_id=paper.bibtex_id, bibtex=paper.bibtex)
        session.add(bib_row)
        session.flush()  # get PK assigned before FK reference

        paper_row = Paper(
            title=paper.title,
            contents=paper.contents,
            bibtex_id=paper.bibtex_id,
        )
        session.add(paper_row)
        session.flush()  # get paper.id

        paper_id = paper_row.id

        for author_name in paper.authors:
            author_row = session.execute(
                select(Author).where(Author.author == author_name)
            ).scalar_one_or_none()
            if author_row is None:
                author_row = Author(author=author_name)
                session.add(author_row)
                session.flush()

            link = AuthorPaper(author_id=author_row.id, paper_id=paper_id)
            session.add(link)

        session.flush()
        return self._build_summary(paper_row, session)

    def update_field(
        self,
        session: Session,
        paper_id: int,
        table: Literal["papers", "bib", "authors_id"],
        field: str,
        value: str,
    ) -> None:
        """Update a single field of a paper-related row.

        :param session: Active SQLAlchemy session.
        :param paper_id: ID from ``papers.id`` (used to locate the row).
        :param table: Which table to update — ``"papers"``, ``"bib"``, or ``"authors_id"``.
        :param field: Column name to update.
        :param value: New value to set.
        :raises ValueError: If the table/field combination is not supported.
        :raises ValueError: If ``table == "bib"`` and the bibtex value already exists.
        """
        match table:
            case "papers":
                if field not in ("title", "contents"):
                    raise ValueError(
                        f"Column {field!r} is not updatable in table 'papers'."
                    )
                session.execute(
                    update(Paper)
                    .where(Paper.id == paper_id)
                    .values(**{field: value})
                )
                logger.info("Updated papers.%s for paper_id=%d", field, paper_id)

            case "bib":
                if field != "bibtex":
                    raise ValueError(
                        f"Only 'bibtex' is updatable in table 'bib'; got {field!r}."
                    )
                paper = session.get(Paper, paper_id)
                if paper is None:
                    raise ValueError(f"Paper {paper_id} not found.")
                # Uniqueness check
                existing = session.execute(
                    select(Bib).where(Bib.bibtex == value)
                ).scalar_one_or_none()
                if existing is not None:
                    raise ValueError("bibtex is unique — value already exists.")
                session.execute(
                    update(Bib)
                    .where(Bib.bibtex_id == paper.bibtex_id)
                    .values(bibtex=value)
                )
                logger.info("Updated bib.bibtex for paper_id=%d", paper_id)

            case "authors_id":
                if field != "author":
                    raise ValueError(
                        f"Only 'author' is updatable in table 'authors_id'; got {field!r}."
                    )
                # identifier here is the old author name (as int paper_id is misleading
                # for authors — we use paper_id as the old_author_name sentinel)
                # Actually the CLI passes the old author name as the identifier.
                # We'll re-use paper_id as old_author_id for this branch.
                # The CLI update path for authors passes the author name as identifier.
                # We store the new name in `value`.
                old_author_id = paper_id
                new_author_row = session.execute(
                    select(Author).where(Author.author == value)
                ).scalar_one_or_none()
                if new_author_row is None:
                    new_author_row = Author(author=value)
                    session.add(new_author_row)
                    session.flush()
                session.execute(
                    update(AuthorPaper)
                    .where(AuthorPaper.author_id == old_author_id)
                    .values(author_id=new_author_row.id)
                )
                logger.info(
                    "Updated author_id=%d to author=%r", old_author_id, value
                )

            case _:
                from typing import assert_never

                assert_never(table)

    def delete_paper(self, session: Session, paper_id: int) -> None:
        """Delete a paper and all its associated author links and bib entry.

        :param session: Active SQLAlchemy session.
        :param paper_id: ID from ``papers.id``.
        :raises ValueError: If no paper with ``paper_id`` exists.
        """
        paper = session.get(Paper, paper_id)
        if paper is None:
            raise ValueError(f"Paper {paper_id} not found.")

        bibtex_id = paper.bibtex_id

        # Delete author-paper links for this paper
        # Also delete orphaned author rows
        author_ids = list(
            session.execute(
                select(AuthorPaper.author_id).where(AuthorPaper.paper_id == paper_id)
            ).scalars().all()
        )

        session.execute(
            delete(AuthorPaper).where(AuthorPaper.paper_id == paper_id)
        )

        # Remove authors who have no more papers
        for aid in author_ids:
            remaining = session.execute(
                select(AuthorPaper).where(AuthorPaper.author_id == aid)
            ).scalars().first()
            if remaining is None:
                session.execute(
                    delete(Author).where(Author.id == aid)
                )

        session.execute(delete(Paper).where(Paper.id == paper_id))

        if bibtex_id:
            session.execute(delete(Bib).where(Bib.bibtex_id == bibtex_id))

        logger.info("Deleted paper_id=%d", paper_id)
