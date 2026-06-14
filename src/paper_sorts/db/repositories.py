"""Persistence-layer repositories and DTOs for paper_sorts.

DTOs (Pydantic models) are the only types that cross the layer boundary —
services depend on these, never on ORM types directly.

Only this module (and other modules under db/) imports sqlalchemy.
"""

from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from paper_sorts.db.models import Author, AuthorPaper, Bib, Paper

# ---------------------------------------------------------------------------
# Data Transfer Objects
# ---------------------------------------------------------------------------


class PaperCreate(BaseModel):
    """DTO for creating a new paper.

    :param title: Publication title.
    :param contents: Summary / abstract text.
    :param bibtex_id: BibTeX citation key (unique identifier).
    :param bibtex: Full BibTeX source string.
    :param authors: List of author names in "Last, First" format.
    """

    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: list[str]


class PaperSummary(BaseModel):
    """DTO representing a retrieved paper with all related data.

    :param id: Internal paper id.
    :param title: Publication title.
    :param authors: List of author names in "Last, First" format.
    :param contents: Summary / abstract text.
    :param bibtex_id: BibTeX citation key.
    :param bibtex: Full BibTeX source string.
    """

    id: int
    title: str
    authors: list[str]
    contents: str
    bibtex_id: str
    bibtex: str


# ---------------------------------------------------------------------------
# Repositories
# ---------------------------------------------------------------------------


class BibRepository:
    """Repository for BibTeX entry persistence.

    :param session: Active SQLAlchemy session.
    """

    def __init__(self, session: Session) -> None:
        """Initialise with an active session."""
        self._session = session

    def add(self, bibtex_id: str, bibtex: str) -> Bib:
        """Persist a new Bib entry and return it.

        :param bibtex_id: BibTeX citation key.
        :param bibtex: Full BibTeX source string.
        :returns: The persisted :class:`Bib` ORM object.
        """
        bib = Bib(bibtex_id=bibtex_id, bibtex=bibtex)
        self._session.add(bib)
        self._session.flush()
        return bib

    def get_by_id(self, bibtex_id: str) -> Bib | None:
        """Retrieve a Bib entry by its BibTeX key.

        :param bibtex_id: The citation key to look up.
        :returns: A :class:`Bib` object or ``None`` if not found.
        """
        return self._session.get(Bib, bibtex_id)

    def exists(self, bibtex_id: str) -> bool:
        """Return True if a Bib entry with this key already exists.

        :param bibtex_id: The citation key to check.
        :returns: ``True`` if present, ``False`` otherwise.
        """
        return self.get_by_id(bibtex_id) is not None


class AuthorRepository:
    """Repository for Author persistence.

    :param session: Active SQLAlchemy session.
    """

    def __init__(self, session: Session) -> None:
        """Initialise with an active session."""
        self._session = session

    def get_or_create(self, name: str) -> Author:
        """Return an existing Author with this name, or create and return a new one.

        :param name: Author name in "Last, First" format.
        :returns: Existing or newly created :class:`Author` ORM object.
        """
        stmt = select(Author).where(Author.author == name)
        existing = self._session.execute(stmt).scalars().first()
        if existing is not None:
            return existing
        author = Author(author=name)
        self._session.add(author)
        self._session.flush()
        return author

    def link_to_paper(self, author_id: int, paper_id: int) -> None:
        """Create an AuthorPaper link record.

        :param author_id: Internal ID of the author.
        :param paper_id: Internal ID of the paper.
        """
        link = AuthorPaper(author_id=author_id, paper_id=paper_id)
        self._session.add(link)

    def get_authors_for_paper(self, paper_id: int) -> list[str]:
        """Return all author names linked to a given paper.

        :param paper_id: Internal paper ID.
        :returns: List of author names in "Last, First" format.
        """
        stmt = (
            select(Author.author)
            .join(AuthorPaper, AuthorPaper.author_id == Author.id)
            .where(AuthorPaper.paper_id == paper_id)
        )
        rows = self._session.execute(stmt).scalars().all()
        return [r for r in rows if r is not None]


class PaperRepository:
    """Repository for Paper persistence and retrieval.

    :param session: Active SQLAlchemy session.
    """

    def __init__(self, session: Session) -> None:
        """Initialise with an active session."""
        self._session = session
        self._author_repo = AuthorRepository(session)
        self._bib_repo = BibRepository(session)

    def _to_summary(self, paper: Paper) -> PaperSummary:
        """Convert a Paper ORM object to a PaperSummary DTO.

        :param paper: An ORM Paper instance.
        :returns: A :class:`PaperSummary` DTO.
        """
        authors = self._author_repo.get_authors_for_paper(paper.id)
        bib = self._bib_repo.get_by_id(paper.bibtex_id or "")
        return PaperSummary(
            id=paper.id,
            title=paper.title or "",
            authors=authors,
            contents=paper.contents or "",
            bibtex_id=paper.bibtex_id or "",
            bibtex=bib.bibtex if bib else "",
        )

    def search_by_title(self, term: str) -> list[PaperSummary]:
        """Search papers by title substring (case-insensitive).

        :param term: Substring to search for in paper titles.
        :returns: List of matching :class:`PaperSummary` DTOs.
        """
        stmt = select(Paper).where(Paper.title.ilike(f"%{term}%"))
        papers = self._session.execute(stmt).scalars().all()
        return [self._to_summary(p) for p in papers]

    def search_by_author(self, term: str) -> list[PaperSummary]:
        """Search papers by author name substring (case-insensitive).

        :param term: Substring to search for in author names.
        :returns: List of matching :class:`PaperSummary` DTOs (may include duplicates
            if a paper has multiple matching authors — callers should deduplicate by id).
        """
        stmt = (
            select(Paper)
            .join(AuthorPaper, AuthorPaper.paper_id == Paper.id)
            .join(Author, Author.id == AuthorPaper.author_id)
            .where(Author.author.ilike(f"%{term}%"))
        )
        papers = self._session.execute(stmt).scalars().unique().all()
        return [self._to_summary(p) for p in papers]

    def get_by_id(self, paper_id: int) -> PaperSummary | None:
        """Retrieve a paper by its internal ID.

        :param paper_id: Internal paper ID.
        :returns: A :class:`PaperSummary` DTO or ``None`` if not found.
        """
        paper = self._session.get(Paper, paper_id)
        if paper is None:
            return None
        return self._to_summary(paper)

    def add(self, data: PaperCreate) -> PaperSummary:
        """Add a new paper with its BibTeX entry and authors.

        :param data: :class:`PaperCreate` DTO with all required fields.
        :returns: :class:`PaperSummary` DTO for the newly created paper.
        :raises ValueError: if a paper with this bibtex_id already exists.
        """
        if self._bib_repo.exists(data.bibtex_id):
            raise ValueError(f"Paper with bibtex_id {data.bibtex_id!r} already exists")
        bib = self._bib_repo.add(data.bibtex_id, data.bibtex)
        paper = Paper(title=data.title, contents=data.contents, bibtex_id=bib.bibtex_id)
        self._session.add(paper)
        self._session.flush()
        for name in data.authors:
            author = self._author_repo.get_or_create(name)
            self._author_repo.link_to_paper(author.id, paper.id)
        return self._to_summary(paper)

    def update_title(self, paper_id: int, new_title: str) -> None:
        """Update the title of a paper.

        :param paper_id: Internal paper ID.
        :param new_title: Replacement title string.
        :raises LookupError: if no paper with this ID exists.
        """
        paper = self._session.get(Paper, paper_id)
        if paper is None:
            raise LookupError(f"Paper {paper_id} not found")
        paper.title = new_title
        self._session.flush()

    def update_contents(self, paper_id: int, new_contents: str) -> None:
        """Update the summary/abstract of a paper.

        :param paper_id: Internal paper ID.
        :param new_contents: Replacement contents string.
        :raises LookupError: if no paper with this ID exists.
        """
        paper = self._session.get(Paper, paper_id)
        if paper is None:
            raise LookupError(f"Paper {paper_id} not found")
        paper.contents = new_contents
        self._session.flush()

    def update_bibtex(self, paper_id: int, new_bibtex: str) -> None:
        """Update the BibTeX source string for a paper.

        :param paper_id: Internal paper ID.
        :param new_bibtex: Replacement BibTeX source string.
        :raises LookupError: if no paper with this ID exists.
        """
        paper = self._session.get(Paper, paper_id)
        if paper is None:
            raise LookupError(f"Paper {paper_id} not found")
        bib = self._bib_repo.get_by_id(paper.bibtex_id or "")
        if bib is not None:
            bib.bibtex = new_bibtex
            self._session.flush()

    def update_author(self, paper_id: int, new_author: str) -> None:
        """Replace all authors of a paper with a single new author.

        :param paper_id: Internal paper ID.
        :param new_author: New author name in "Last, First" format.
        :raises LookupError: if no paper with this ID exists.
        """
        paper = self._session.get(Paper, paper_id)
        if paper is None:
            raise LookupError(f"Paper {paper_id} not found")
        # Remove existing author links
        stmt = select(AuthorPaper).where(AuthorPaper.paper_id == paper_id)
        links = self._session.execute(stmt).scalars().all()
        for link in links:
            self._session.delete(link)
        self._session.flush()
        author = self._author_repo.get_or_create(new_author)
        self._author_repo.link_to_paper(author.id, paper_id)
        self._session.flush()

    def delete(self, paper_id: int) -> None:
        """Delete a paper and its author links from the database.

        The BibTeX entry (bib table) is left intact (shared resource).

        :param paper_id: Internal paper ID.
        :raises LookupError: if no paper with this ID exists.
        """
        paper = self._session.get(Paper, paper_id)
        if paper is None:
            raise LookupError(f"Paper {paper_id} not found")
        # Remove author links first
        stmt = select(AuthorPaper).where(AuthorPaper.paper_id == paper_id)
        links = self._session.execute(stmt).scalars().all()
        for link in links:
            self._session.delete(link)
        self._session.flush()
        self._session.delete(paper)
        self._session.flush()
