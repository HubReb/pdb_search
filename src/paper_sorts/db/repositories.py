"""Repository classes and Pydantic DTOs for paper_sorts persistence layer.

Services interact with the database only through these repositories.
ORM model instances never leave this module — only DTOs cross the boundary.

Only this module (and the rest of db/) may import sqlalchemy.
"""

import logging

from pydantic import BaseModel
from sqlalchemy import delete, select, update
from sqlalchemy.orm import Session

from paper_sorts.db.models import Author, AuthorPaper, Bib, Paper

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DTOs
# ---------------------------------------------------------------------------


class PaperSummary(BaseModel):
    """Read-only view of a paper including authors and bibtex entry."""

    id: int
    title: str
    contents: str
    bibtex_id: str
    authors: list[str]
    bibtex: str


class PaperCreate(BaseModel):
    """Data required to insert a new paper."""

    title: str
    contents: str
    bibtex_id: str
    authors: list[str]
    bibtex: str


# ---------------------------------------------------------------------------
# BibRepository
# ---------------------------------------------------------------------------


class BibRepository:
    """CRUD operations on the bib table."""

    def __init__(self, session: Session) -> None:
        """Initialise with an open SQLAlchemy session.

        :param session: Active database session.
        """
        self._session = session

    def get(self, bibtex_id: str) -> str | None:
        """Return the bibtex string for *bibtex_id*, or None if absent.

        :param bibtex_id: BibTeX citation key.
        :returns: Full BibTeX string, or ``None``.
        """
        row = self._session.get(Bib, bibtex_id)
        return row.bibtex if row else None

    def add(self, bibtex_id: str, bibtex: str) -> None:
        """Insert a new bib entry.

        :param bibtex_id: BibTeX citation key (must be unique).
        :param bibtex: Full BibTeX source string (must be unique).
        :raises ValueError: If bibtex_id or bibtex already exists.
        """
        existing = self._session.get(Bib, bibtex_id)
        if existing:
            raise ValueError(f"bibtex_id '{bibtex_id}' already exists in bib table")
        entry = Bib(bibtex_id=bibtex_id, bibtex=bibtex)
        self._session.add(entry)
        self._session.flush()

    def update_bibtex(self, bibtex_id: str, new_bibtex: str) -> None:
        """Update the bibtex string for an existing bib entry.

        :param bibtex_id: Identifies the row to update.
        :param new_bibtex: Replacement BibTeX string (must be unique).
        :raises ValueError: If bibtex_id does not exist, or new_bibtex is already used.
        """
        # Check uniqueness of new value
        stmt = select(Bib).where(Bib.bibtex == new_bibtex)
        clash = self._session.scalar(stmt)
        if clash:
            raise ValueError(f"bibtex value already exists for key '{clash.bibtex_id}'")
        stmt_upd = update(Bib).where(Bib.bibtex_id == bibtex_id).values(bibtex=new_bibtex)
        self._session.execute(stmt_upd)

    def delete(self, bibtex_id: str) -> None:
        """Delete bib entry by bibtex_id.

        :param bibtex_id: BibTeX citation key to remove.
        """
        self._session.execute(delete(Bib).where(Bib.bibtex_id == bibtex_id))


# ---------------------------------------------------------------------------
# AuthorRepository
# ---------------------------------------------------------------------------


class AuthorRepository:
    """CRUD operations on the authors_id and authors_papers tables."""

    def __init__(self, session: Session) -> None:
        """Initialise with an open SQLAlchemy session.

        :param session: Active database session.
        """
        self._session = session

    def get_or_create(self, name: str) -> int:
        """Return the author's id, creating the author if absent.

        :param name: Author name in "Last, First" form.
        :returns: The ``authors_id.id`` for this author.
        """
        stmt = select(Author).where(Author.author == name)
        row = self._session.scalar(stmt)
        if row:
            return row.id
        author = Author(author=name)
        self._session.add(author)
        self._session.flush()
        return author.id

    def get_names_for_paper(self, paper_id: int) -> list[str]:
        """Return author names for a given paper.

        :param paper_id: The ``papers.id`` of the paper.
        :returns: List of author name strings.
        """
        stmt = (
            select(Author.author)
            .join(AuthorPaper, Author.id == AuthorPaper.author_id)
            .where(AuthorPaper.paper_id == paper_id)
        )
        return list(self._session.scalars(stmt))

    def link_author_to_paper(self, author_id: int, paper_id: int) -> None:
        """Create an authors_papers link row.

        :param author_id: The ``authors_id.id`` to link.
        :param paper_id: The ``papers.id`` to link.
        """
        link = AuthorPaper(author_id=author_id, paper_id=paper_id)
        self._session.add(link)
        self._session.flush()

    def unlink_paper(self, paper_id: int) -> None:
        """Remove all authorship links for *paper_id*.

        Also removes author rows that have no remaining paper links.

        :param paper_id: The ``papers.id`` whose links to remove.
        """
        # Fetch author IDs linked to this paper before deletion
        stmt = select(AuthorPaper.author_id).where(AuthorPaper.paper_id == paper_id)
        author_ids = list(self._session.scalars(stmt))

        # Remove links
        self._session.execute(
            delete(AuthorPaper).where(AuthorPaper.paper_id == paper_id)
        )
        self._session.flush()

        # Remove authors who have no remaining links
        for aid in author_ids:
            remaining = self._session.scalar(
                select(AuthorPaper).where(AuthorPaper.author_id == aid)
            )
            if not remaining:
                self._session.execute(delete(Author).where(Author.id == aid))

    def update_author_name(self, author_id: int, new_name: str) -> None:
        """Update the author's display name.

        :param author_id: The ``authors_id.id`` to update.
        :param new_name: Replacement name string.
        """
        self._session.execute(
            update(Author).where(Author.id == author_id).values(author=new_name)
        )


# ---------------------------------------------------------------------------
# PaperRepository
# ---------------------------------------------------------------------------


class PaperRepository:
    """CRUD and search operations on the papers table.

    Composes :class:`AuthorRepository` and :class:`BibRepository` to build
    complete :class:`PaperSummary` DTOs.
    """

    def __init__(self, session: Session) -> None:
        """Initialise with an open SQLAlchemy session.

        :param session: Active database session.
        """
        self._session = session
        self._authors = AuthorRepository(session)
        self._bibs = BibRepository(session)

    def _to_summary(self, paper: Paper) -> PaperSummary:
        """Build a PaperSummary DTO from an ORM Paper row.

        :param paper: ORM model instance.
        :returns: Fully-populated :class:`PaperSummary`.
        """
        authors = self._authors.get_names_for_paper(paper.id)
        bibtex = self._bibs.get(paper.bibtex_id) or ""
        return PaperSummary(
            id=paper.id,
            title=paper.title,
            contents=paper.contents,
            bibtex_id=paper.bibtex_id,
            authors=authors,
            bibtex=bibtex,
        )

    def search_by_title(self, title: str) -> list[PaperSummary]:
        """Return all papers whose title exactly matches *title*.

        :param title: Exact paper title to search for.
        :returns: List of matching :class:`PaperSummary` DTOs (may be empty).
        """
        stmt = select(Paper).where(Paper.title == title)
        papers = list(self._session.scalars(stmt))
        return [self._to_summary(p) for p in papers]

    def search_by_author(self, author_name: str) -> list[PaperSummary]:
        """Return all papers by the given author.

        :param author_name: Author name in "Last, First" form.
        :returns: List of matching :class:`PaperSummary` DTOs (may be empty).
        """
        stmt = (
            select(Paper)
            .join(AuthorPaper, Paper.id == AuthorPaper.paper_id)
            .join(Author, AuthorPaper.author_id == Author.id)
            .where(Author.author == author_name)
        )
        papers = list(self._session.scalars(stmt))
        return [self._to_summary(p) for p in papers]

    def add(self, paper: PaperCreate) -> None:
        """Insert a new paper with its authors and bibtex entry.

        Skips insertion if bibtex_id already exists.

        :param paper: :class:`PaperCreate` DTO with all required fields.
        :raises ValueError: If bibtex_id already exists in the database.
        """
        existing = self._session.get(Bib, paper.bibtex_id)
        if existing:
            raise ValueError(f"bibtex_id '{paper.bibtex_id}' already exists")

        # Insert bib entry
        self._bibs.add(paper.bibtex_id, paper.bibtex)

        # Insert paper row
        paper_row = Paper(
            title=paper.title,
            contents=paper.contents,
            bibtex_id=paper.bibtex_id,
        )
        self._session.add(paper_row)
        self._session.flush()

        # Insert authors and links
        for author_name in paper.authors:
            author_id = self._authors.get_or_create(author_name)
            self._authors.link_author_to_paper(author_id, paper_row.id)

    def delete(self, paper_id: int) -> None:
        """Delete a paper and all associated authors/authorship links/bib entry.

        :param paper_id: The ``papers.id`` to delete.
        :raises ValueError: If no paper with *paper_id* exists.
        """
        paper = self._session.get(Paper, paper_id)
        if not paper:
            raise ValueError(f"Paper id={paper_id} not found")
        bibtex_id = paper.bibtex_id

        # Remove authorship links (and orphaned authors)
        self._authors.unlink_paper(paper_id)

        # Remove paper row
        self._session.execute(delete(Paper).where(Paper.id == paper_id))
        self._session.flush()

        # Remove bib entry
        self._bibs.delete(bibtex_id)

    def update_field(
        self,
        table: str,
        column: str,
        identifier: str,
        value: str,
    ) -> None:
        """Update a single field in the specified table.

        :param table: One of ``"papers"``, ``"bib"``, ``"authors_id"``.
        :param column: Column name to update.
        :param identifier: Row identifier (paper id, bibtex_id, or author id as str).
        :param value: New value for the column.
        :raises ValueError: If table/column combination is unsupported, or
            the identifier is not found.
        """
        match table:
            case "papers":
                self._update_papers(column, identifier, value)
            case "bib":
                self._update_bib(column, identifier, value)
            case "authors_id":
                self._update_author(column, identifier, value)
            case _:
                raise ValueError(f"Unsupported table: '{table}'")

    def _update_papers(self, column: str, identifier: str, value: str) -> None:
        """Update papers.title or papers.contents by paper id.

        :param column: ``"title"`` or ``"contents"``.
        :param identifier: String representation of papers.id.
        :param value: New value.
        :raises ValueError: If column is not editable or paper not found.
        """
        try:
            paper_id = int(identifier)
        except ValueError as exc:
            raise ValueError(f"Paper identifier must be an integer, got '{identifier}'") from exc

        match column:
            case "title":
                field = Paper.title
            case "contents":
                field = Paper.contents
            case _:
                raise ValueError(f"Column '{column}' is not editable in table papers")

        from sqlalchemy.engine import CursorResult

        result: CursorResult[tuple[()]] = self._session.execute(  # type: ignore[assignment]
            update(Paper).where(Paper.id == paper_id).values({field: value})
        )
        if result.rowcount == 0:
            raise ValueError(f"Paper id={paper_id} not found")

    def _update_bib(self, column: str, bibtex_id: str, value: str) -> None:
        """Update bib.bibtex by bibtex_id.

        :param column: Must be ``"bibtex"``.
        :param bibtex_id: The bib entry key.
        :param value: New BibTeX string.
        :raises ValueError: If column is not ``"bibtex"`` or key not found.
        """
        match column:
            case "bibtex":
                self._bibs.update_bibtex(bibtex_id, value)
            case _:
                raise ValueError(f"Column '{column}' is not editable in table bib")

    def _update_author(self, column: str, identifier: str, value: str) -> None:
        """Update authors_id.author by author id.

        :param column: Must be ``"author"``.
        :param identifier: String representation of authors_id.id.
        :param value: New author name.
        :raises ValueError: If column is not ``"author"`` or author not found.
        """
        try:
            author_id = int(identifier)
        except ValueError as exc:
            raise ValueError(
                f"Author identifier must be an integer, got '{identifier}'"
            ) from exc

        match column:
            case "author":
                self._authors.update_author_name(author_id, value)
            case _:
                raise ValueError(f"Column '{column}' is not editable in table authors_id")
