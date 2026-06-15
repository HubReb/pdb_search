"""Repository classes and DTOs for paper_sorts persistence layer.

Only this module and db/session.py may import sqlalchemy or any database
driver (constitution Principle I).

DTOs (PaperCreate, PaperSummary) are Pydantic models that flow between
the service layer and the persistence layer. Services depend on DTOs —
never on ORM types.
"""

from __future__ import annotations

import logging

from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from paper_sorts.db.models import Author, AuthorPaper, Bib, Paper

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data Transfer Objects
# ---------------------------------------------------------------------------


class PaperCreate(BaseModel):
    """DTO carrying all data needed to insert a new paper.

    :param title: Publication title.
    :param contents: One-sentence summary.
    :param bibtex_id: Unique BibTeX key (user-facing identifier).
    :param bibtex: Full BibTeX source string.
    :param authors: List of author names in 'Last, First' form.
    """

    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: list[str]


class PaperSummary(BaseModel):
    """DTO returned by search operations.

    :param paper_id: Internal integer ID.
    :param title: Publication title.
    :param contents: One-sentence summary.
    :param bibtex_id: Unique BibTeX key.
    :param bibtex: Full BibTeX source string.
    :param authors: List of author names in 'Last, First' form.
    """

    paper_id: int
    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: list[str]


# ---------------------------------------------------------------------------
# BibTeX repository
# ---------------------------------------------------------------------------


class BibRepository:
    """Persistence operations for the bib table.

    :param session: Active SQLAlchemy session (caller owns lifecycle).
    """

    def __init__(self, session: Session) -> None:
        """Initialise with a caller-managed session.

        :param session: Active SQLAlchemy session.
        """
        self.session = session

    def add(self, bibtex_id: str, bibtex: str) -> Bib:
        """Insert a new BibTeX entry.

        :param bibtex_id: Unique BibTeX key.
        :param bibtex: Full BibTeX source string.
        :return: The newly created Bib ORM object.
        :raises ValueError: If bibtex_id already exists.
        """
        existing = self.session.get(Bib, bibtex_id)
        if existing is not None:
            raise ValueError(f"BibTeX entry '{bibtex_id}' already exists.")
        entry = Bib(bibtex_id=bibtex_id, bibtex=bibtex)
        self.session.add(entry)
        self.session.flush()
        return entry

    def find_by_id(self, bibtex_id: str) -> Bib | None:
        """Look up a BibTeX entry by key.

        :param bibtex_id: BibTeX key to look up.
        :return: Bib instance or None if not found.
        """
        return self.session.get(Bib, bibtex_id)

    def update(self, bibtex_id: str, new_bibtex: str) -> None:
        """Replace the bibtex field for an existing entry.

        :param bibtex_id: BibTeX key of the entry to update.
        :param new_bibtex: New BibTeX source string.
        :raises ValueError: If the entry does not exist.
        """
        entry = self.session.get(Bib, bibtex_id)
        if entry is None:
            raise ValueError(f"BibTeX entry '{bibtex_id}' not found.")
        entry.bibtex = new_bibtex
        self.session.flush()


# ---------------------------------------------------------------------------
# Author repository
# ---------------------------------------------------------------------------


class AuthorRepository:
    """Persistence operations for the authors_id and authors_papers tables.

    :param session: Active SQLAlchemy session (caller owns lifecycle).
    """

    def __init__(self, session: Session) -> None:
        """Initialise with a caller-managed session.

        :param session: Active SQLAlchemy session.
        """
        self.session = session

    def find_or_create(self, name: str) -> Author:
        """Return an existing author by name or create a new one.

        Two authors with identical names are treated as the same author
        (known limitation, documented in architecture.md).

        :param name: Author name in 'Last, First' form.
        :return: Author ORM instance.
        """
        stmt = select(Author).where(Author.author == name)
        author = self.session.scalars(stmt).first()
        if author is None:
            author = Author(author=name)
            self.session.add(author)
            self.session.flush()
        return author

    def link_to_paper(self, author: Author, paper: Paper) -> None:
        """Create an AuthorPaper link between an author and a paper.

        :param author: Author ORM instance.
        :param paper: Paper ORM instance.
        """
        link = AuthorPaper(author_id=author.id, paper_id=paper.id)
        self.session.add(link)
        self.session.flush()

    def update_name(self, old_name: str, new_name: str) -> None:
        """Rename an author in place (all papers keep their links).

        :param old_name: Current author name.
        :param new_name: New author name.
        :raises ValueError: If no author with old_name exists.
        """
        stmt = select(Author).where(Author.author == old_name)
        author = self.session.scalars(stmt).first()
        if author is None:
            raise ValueError(f"Author '{old_name}' not found.")
        author.author = new_name
        self.session.flush()

    def get_authors_for_paper(self, paper_id: int) -> list[str]:
        """Return all author names for a given paper.

        :param paper_id: Internal paper ID.
        :return: List of author name strings.
        """
        stmt = (
            select(Author.author)
            .join(AuthorPaper, AuthorPaper.author_id == Author.id)
            .where(AuthorPaper.paper_id == paper_id)
        )
        return list(self.session.scalars(stmt).all())


# ---------------------------------------------------------------------------
# Paper repository
# ---------------------------------------------------------------------------


class PaperRepository:
    """Persistence operations for the papers table and related joins.

    :param session: Active SQLAlchemy session (caller owns lifecycle).
    """

    def __init__(self, session: Session) -> None:
        """Initialise with a caller-managed session.

        :param session: Active SQLAlchemy session.
        """
        self.session = session
        self._bib_repo = BibRepository(session)
        self._author_repo = AuthorRepository(session)

    def _paper_to_summary(self, paper: Paper) -> PaperSummary:
        """Convert a Paper ORM object to a PaperSummary DTO.

        Fetches the BibTeX entry and author list explicitly (no ORM relationships,
        since authors_papers has no DDL FKs).

        :param paper: ORM Paper instance.
        :return: PaperSummary DTO.
        """
        authors = self._author_repo.get_authors_for_paper(paper.id)
        bib = self._bib_repo.find_by_id(paper.bibtex_id)
        bibtex_text = bib.bibtex if bib is not None else ""
        return PaperSummary(
            paper_id=paper.id,
            title=paper.title,
            contents=paper.contents,
            bibtex_id=paper.bibtex_id,
            bibtex=bibtex_text,
            authors=authors,
        )

    def search_by_title(self, title: str) -> list[PaperSummary]:
        """Search for papers whose title matches exactly.

        :param title: Exact paper title to search for.
        :return: List of PaperSummary DTOs (may be empty).
        """
        stmt = select(Paper).where(Paper.title == title)
        papers = list(self.session.scalars(stmt).all())
        return [self._paper_to_summary(p) for p in papers]

    def search_by_author(self, author_name: str) -> list[PaperSummary]:
        """Search for papers by author name.

        :param author_name: Author name (exact match, 'Last, First' form).
        :return: List of PaperSummary DTOs (may be empty).
        """
        stmt = (
            select(Paper)
            .join(AuthorPaper, AuthorPaper.paper_id == Paper.id)
            .join(Author, Author.id == AuthorPaper.author_id)
            .where(Author.author == author_name)
        )
        papers = list(self.session.scalars(stmt).all())
        return [self._paper_to_summary(p) for p in papers]

    def add(self, data: PaperCreate) -> Paper:
        """Insert a new paper with its BibTeX entry and authors.

        :param data: PaperCreate DTO with all required fields.
        :return: The newly created Paper ORM instance.
        :raises ValueError: If bibtex_id already exists.
        """
        bib = self._bib_repo.add(data.bibtex_id, data.bibtex)
        paper = Paper(title=data.title, contents=data.contents, bibtex_id=bib.bibtex_id)
        self.session.add(paper)
        self.session.flush()
        for name in data.authors:
            author = self._author_repo.find_or_create(name)
            self._author_repo.link_to_paper(author, paper)
        self.session.flush()
        return paper

    def find_by_bibtex_id(self, bibtex_id: str) -> Paper | None:
        """Find a paper by its BibTeX key.

        :param bibtex_id: BibTeX key.
        :return: Paper ORM instance or None.
        """
        stmt = select(Paper).where(Paper.bibtex_id == bibtex_id)
        return self.session.scalars(stmt).first()

    def update_title(self, paper_id: int, new_title: str) -> None:
        """Update a paper's title.

        :param paper_id: Internal paper ID.
        :param new_title: Replacement title.
        :raises ValueError: If paper not found.
        """
        paper = self.session.get(Paper, paper_id)
        if paper is None:
            raise ValueError(f"Paper with id {paper_id} not found.")
        paper.title = new_title
        self.session.flush()

    def update_contents(self, paper_id: int, new_contents: str) -> None:
        """Update a paper's summary (contents).

        :param paper_id: Internal paper ID.
        :param new_contents: Replacement summary.
        :raises ValueError: If paper not found.
        """
        paper = self.session.get(Paper, paper_id)
        if paper is None:
            raise ValueError(f"Paper with id {paper_id} not found.")
        paper.contents = new_contents
        self.session.flush()

    def delete(self, bibtex_id: str) -> bool:
        """Delete a paper and all related AuthorPaper links and Bib entry.

        Does NOT delete authors from authors_id — they may be linked to other
        papers (matches legacy DatabaseConnector behaviour).

        :param bibtex_id: BibTeX key of the paper to delete.
        :return: True if deleted, False if not found.
        """
        paper = self.find_by_bibtex_id(bibtex_id)
        if paper is None:
            return False
        # Remove author links
        stmt = select(AuthorPaper).where(AuthorPaper.paper_id == paper.id)
        for link in self.session.scalars(stmt).all():
            self.session.delete(link)
        # Remove paper
        self.session.delete(paper)
        # Remove bib entry
        bib = self.session.get(Bib, bibtex_id)
        if bib is not None:
            self.session.delete(bib)
        self.session.flush()
        return True
