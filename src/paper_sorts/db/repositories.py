"""Repository classes and Pydantic DTOs for paper_sorts persistence layer.

Only this module (and the rest of src/paper_sorts/db/) may import sqlalchemy.
Service and CLI layers interact only with the DTOs and repository methods
defined here.
"""

from __future__ import annotations

from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from paper_sorts.db.models import Author, AuthorPaper, Bib, Paper

# ---------------------------------------------------------------------------
# DTOs
# ---------------------------------------------------------------------------


class PaperSummary(BaseModel):
    """Read-model DTO returned by repository search operations.

    Attributes:
        id: Surrogate primary key.
        title: Paper title.
        contents: Summary / abstract.
        bibtex_id: BibTeX citation key.
        bibtex: Full BibTeX source string.
        authors: List of author names in 'Last, First' form.
    """

    id: int
    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: list[str]


class PaperCreate(BaseModel):
    """Write-model DTO for add operations.

    Attributes:
        title: Paper title. Must be non-empty.
        contents: Summary / abstract. Must be non-empty.
        bibtex_id: BibTeX citation key. Must be non-empty and unique.
        bibtex: Full BibTeX source string.
        authors: List of author names. Must have at least one entry.
    """

    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: list[str]


# ---------------------------------------------------------------------------
# Repositories
# ---------------------------------------------------------------------------


class BibRepository:
    """Repository for BibTeX entry persistence.

    Args:
        session: An open SQLAlchemy Session.
    """

    def __init__(self, session: Session) -> None:
        """Initialise with an open session."""
        self._session = session

    def add(self, bibtex_id: str, bibtex: str) -> Bib:
        """Insert a new BibTeX entry.

        Args:
            bibtex_id: The citation key.
            bibtex: The full BibTeX source string.

        Returns:
            The newly created Bib ORM object.
        """
        bib = Bib(bibtex_id=bibtex_id, bibtex=bibtex)
        self._session.add(bib)
        self._session.flush()
        return bib

    def get(self, bibtex_id: str) -> Bib | None:
        """Retrieve a BibTeX entry by key.

        Args:
            bibtex_id: The citation key to look up.

        Returns:
            The matching Bib ORM object, or None if not found.
        """
        return self._session.get(Bib, bibtex_id)


class AuthorRepository:
    """Repository for author record persistence.

    Args:
        session: An open SQLAlchemy Session.
    """

    def __init__(self, session: Session) -> None:
        """Initialise with an open session."""
        self._session = session

    def get_or_create(self, name: str) -> Author:
        """Return existing author with matching name, or create one.

        Args:
            name: Author name in 'Last, First' form.

        Returns:
            Existing or newly created Author ORM object.
        """
        stmt = select(Author).where(Author.author == name)
        existing = self._session.execute(stmt).scalar_one_or_none()
        if existing is not None:
            return existing
        author = Author(author=name)
        self._session.add(author)
        self._session.flush()
        return author

    def link_to_paper(self, author_id: int, paper_id: int) -> None:
        """Insert a row into authors_papers linking author to paper.

        Args:
            author_id: The authors_id.id value.
            paper_id: The papers.id value.
        """
        link = AuthorPaper(author_id=author_id, paper_id=paper_id)
        self._session.add(link)

    def get_authors_for_paper(self, paper_id: int) -> list[str]:
        """Return list of author names for the given paper.

        Args:
            paper_id: The papers.id value.

        Returns:
            List of author name strings, possibly empty.
        """
        stmt = (
            select(Author.author)
            .join(AuthorPaper, AuthorPaper.author_id == Author.id)
            .where(AuthorPaper.paper_id == paper_id)
        )
        rows = self._session.execute(stmt).scalars().all()
        return [r for r in rows if r is not None]

    def delete_links_for_paper(self, paper_id: int) -> None:
        """Remove all authorship links for the given paper.

        Args:
            paper_id: The papers.id value.
        """
        stmt = select(AuthorPaper).where(AuthorPaper.paper_id == paper_id)
        links = self._session.execute(stmt).scalars().all()
        for link in links:
            self._session.delete(link)


class PaperRepository:
    """Repository for paper record persistence and search.

    Args:
        session: An open SQLAlchemy Session.
    """

    def __init__(self, session: Session) -> None:
        """Initialise with an open session."""
        self._session = session
        self._bib_repo = BibRepository(session)
        self._author_repo = AuthorRepository(session)

    def add(self, paper: PaperCreate) -> PaperSummary:
        """Insert a new paper with its BibTeX entry and authors.

        Inserts into bib, papers, authors_id, and authors_papers in one
        flush; all within the caller's session (commit happens in with_session).

        Args:
            paper: PaperCreate DTO with all required fields non-empty.

        Returns:
            PaperSummary DTO for the newly created paper.

        Raises:
            sqlalchemy.exc.IntegrityError: If bibtex_id or bibtex already exists.
        """
        bib = self._bib_repo.add(paper.bibtex_id, paper.bibtex)
        db_paper = Paper(
            title=paper.title,
            contents=paper.contents,
            bibtex_id=bib.bibtex_id,
        )
        self._session.add(db_paper)
        self._session.flush()

        for name in paper.authors:
            author = self._author_repo.get_or_create(name)
            self._author_repo.link_to_paper(author.id, db_paper.id)

        return PaperSummary(
            id=db_paper.id,
            title=paper.title,
            contents=paper.contents,
            bibtex_id=paper.bibtex_id,
            bibtex=paper.bibtex,
            authors=list(paper.authors),
        )

    def search_by_title(self, title: str) -> list[PaperSummary]:
        """Find papers whose title contains the given substring (case-insensitive).

        Args:
            title: Title search string (partial match).

        Returns:
            List of PaperSummary DTOs, possibly empty.
        """
        stmt = (
            select(Paper, Bib)
            .join(Bib, Bib.bibtex_id == Paper.bibtex_id, isouter=True)
            .where(Paper.title.ilike(f"%{title}%"))
        )
        rows = self._session.execute(stmt).all()
        return [self._to_summary(paper, bib) for paper, bib in rows]

    def search_by_author(self, author_name: str) -> list[PaperSummary]:
        """Find papers with an author whose name contains the search string.

        Args:
            author_name: Author name search string (partial match).

        Returns:
            List of PaperSummary DTOs, possibly empty.
        """
        stmt = (
            select(Paper, Bib)
            .join(Bib, Bib.bibtex_id == Paper.bibtex_id, isouter=True)
            .join(AuthorPaper, AuthorPaper.paper_id == Paper.id)
            .join(Author, Author.id == AuthorPaper.author_id)
            .where(Author.author.ilike(f"%{author_name}%"))
        )
        rows = self._session.execute(stmt).all()
        return [self._to_summary(paper, bib) for paper, bib in rows]

    def get_by_id(self, paper_id: int) -> PaperSummary | None:
        """Retrieve a paper by surrogate id.

        Args:
            paper_id: The papers.id value.

        Returns:
            PaperSummary DTO, or None if not found.
        """
        paper = self._session.get(Paper, paper_id)
        if paper is None:
            return None
        bib = self._bib_repo.get(paper.bibtex_id or "")
        return self._to_summary(paper, bib)

    def update_title(self, paper_id: int, new_title: str) -> PaperSummary:
        """Update the title of a paper.

        Args:
            paper_id: The papers.id value.
            new_title: Replacement title string.

        Returns:
            Updated PaperSummary DTO.

        Raises:
            ValueError: If no paper with the given id exists.
        """
        paper = self._get_paper_or_raise(paper_id)
        paper.title = new_title
        self._session.flush()
        return self._to_summary(paper, self._bib_repo.get(paper.bibtex_id or ""))

    def update_contents(self, paper_id: int, new_contents: str) -> PaperSummary:
        """Update the contents (summary) of a paper.

        Args:
            paper_id: The papers.id value.
            new_contents: Replacement summary string.

        Returns:
            Updated PaperSummary DTO.

        Raises:
            ValueError: If no paper with the given id exists.
        """
        paper = self._get_paper_or_raise(paper_id)
        paper.contents = new_contents
        self._session.flush()
        return self._to_summary(paper, self._bib_repo.get(paper.bibtex_id or ""))

    def update_bibtex(self, paper_id: int, new_bibtex: str) -> PaperSummary:
        """Replace the BibTeX source string for a paper.

        Args:
            paper_id: The papers.id value.
            new_bibtex: Replacement BibTeX source string.

        Returns:
            Updated PaperSummary DTO.

        Raises:
            ValueError: If no paper with the given id exists.
        """
        paper = self._get_paper_or_raise(paper_id)
        bib = self._bib_repo.get(paper.bibtex_id or "")
        if bib is not None:
            bib.bibtex = new_bibtex
            self._session.flush()
        return self._to_summary(paper, bib)

    def update_authors(self, paper_id: int, new_authors: list[str]) -> PaperSummary:
        """Replace the author list for a paper.

        Removes all existing authorship links and inserts new ones.

        Args:
            paper_id: The papers.id value.
            new_authors: Replacement list of author name strings.

        Returns:
            Updated PaperSummary DTO.

        Raises:
            ValueError: If no paper with the given id exists.
        """
        paper = self._get_paper_or_raise(paper_id)
        self._author_repo.delete_links_for_paper(paper_id)
        for name in new_authors:
            author = self._author_repo.get_or_create(name)
            self._author_repo.link_to_paper(author.id, paper_id)
        self._session.flush()
        bib = self._bib_repo.get(paper.bibtex_id or "")
        return self._to_summary(paper, bib)

    def delete(self, paper_id: int) -> None:
        """Delete a paper and its authorship links.

        Does not remove the BibTeX entry from bib (may be shared or retained).

        Args:
            paper_id: The papers.id value.

        Raises:
            ValueError: If no paper with the given id exists.
        """
        paper = self._get_paper_or_raise(paper_id)
        self._author_repo.delete_links_for_paper(paper_id)
        self._session.delete(paper)
        self._session.flush()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _get_paper_or_raise(self, paper_id: int) -> Paper:
        """Return Paper ORM object or raise ValueError.

        Args:
            paper_id: The papers.id value.

        Returns:
            Paper ORM object.

        Raises:
            ValueError: If no paper with the given id exists.
        """
        paper = self._session.get(Paper, paper_id)
        if paper is None:
            raise ValueError(f"No paper with id={paper_id}")
        return paper

    def _to_summary(self, paper: Paper, bib: Bib | None) -> PaperSummary:
        """Convert ORM Paper + Bib to a PaperSummary DTO.

        Args:
            paper: Paper ORM object.
            bib: Associated Bib ORM object, or None.

        Returns:
            PaperSummary DTO populated from ORM objects.
        """
        authors = self._author_repo.get_authors_for_paper(paper.id)
        return PaperSummary(
            id=paper.id,
            title=paper.title or "",
            contents=paper.contents or "",
            bibtex_id=paper.bibtex_id or "",
            bibtex=bib.bibtex or "" if bib else "",
            authors=authors,
        )
