"""Repository classes and DTOs for paper_sorts persistence layer.

Only modules under src/paper_sorts/db/ may import sqlalchemy.
Services depend on the DTOs (PaperCreate, PaperSummary) exported here;
they never touch ORM types directly.
"""

from __future__ import annotations

import logging

from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from paper_sorts.db.models import Author, AuthorPaper, BibEntry, Paper

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DTOs — the types services and CLI depend on (no ORM types leak upward)
# ---------------------------------------------------------------------------


class PaperCreate(BaseModel):
    """DTO used to create a new paper entry.

    :param title: publication title
    :param contents: summary or abstract
    :param bibtex_id: BibTeX citation key (user-facing unique identifier)
    :param bibtex: full BibTeX source string
    :param authors: list of author names in 'Last, First' form
    """

    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: list[str]


class PaperSummary(BaseModel):
    """DTO returned by search operations.

    :param id: internal row identifier
    :param title: publication title
    :param contents: summary or abstract
    :param bibtex_id: BibTeX citation key
    :param authors: list of author names in 'Last, First' form
    :param bibtex: full BibTeX source string
    """

    id: int
    title: str
    contents: str
    bibtex_id: str
    authors: list[str]
    bibtex: str


# ---------------------------------------------------------------------------
# BibRepository
# ---------------------------------------------------------------------------


class BibRepository:
    """Persistence operations for the bib table.

    :param session: active SQLAlchemy session (injected by caller)
    """

    def __init__(self, session: Session) -> None:
        """Initialise with the given session.

        :param session: SQLAlchemy Session to use for all operations
        """
        self.session = session

    def get_by_id(self, bibtex_id: str) -> BibEntry | None:
        """Fetch a BibEntry by its primary key.

        :param bibtex_id: BibTeX citation key
        :return: BibEntry row, or None if not found
        """
        return self.session.get(BibEntry, bibtex_id)

    def get_or_create(self, bibtex_id: str, bibtex: str) -> BibEntry:
        """Return existing BibEntry or create a new one.

        :param bibtex_id: BibTeX citation key
        :param bibtex: full BibTeX source string
        :return: existing or newly created BibEntry
        """
        existing = self.get_by_id(bibtex_id)
        if existing is not None:
            return existing
        entry = BibEntry(bibtex_id=bibtex_id, bibtex=bibtex)
        self.session.add(entry)
        self.session.flush()
        return entry

    def update(self, bibtex_id: str, new_bibtex: str) -> BibEntry:
        """Update the bibtex field for an existing BibEntry.

        :param bibtex_id: BibTeX citation key to update
        :param new_bibtex: new BibTeX source string
        :return: updated BibEntry
        :raises KeyError: if no entry found for the given bibtex_id
        :raises ValueError: if new_bibtex already exists in another row (UNIQUE constraint)
        """
        entry = self.get_by_id(bibtex_id)
        if entry is None:
            raise KeyError(f"BibTeX entry '{bibtex_id}' not found.")
        # Check uniqueness manually to give a clear error before DB rejects it
        stmt = select(BibEntry).where(
            BibEntry.bibtex == new_bibtex, BibEntry.bibtex_id != bibtex_id
        )
        existing = self.session.execute(stmt).scalar_one_or_none()
        if existing is not None:
            raise ValueError("BibTeX string already exists for another entry.")
        entry.bibtex = new_bibtex
        self.session.flush()
        return entry


# ---------------------------------------------------------------------------
# AuthorRepository
# ---------------------------------------------------------------------------


class AuthorRepository:
    """Persistence operations for the authors_id and authors_papers tables.

    :param session: active SQLAlchemy session (injected by caller)
    """

    def __init__(self, session: Session) -> None:
        """Initialise with the given session.

        :param session: SQLAlchemy Session to use for all operations
        """
        self.session = session

    def get_by_name(self, name: str) -> Author | None:
        """Fetch an Author by name.

        :param name: author name in 'Last, First' form
        :return: Author row, or None if not found
        """
        stmt = select(Author).where(Author.author == name)
        return self.session.execute(stmt).scalar_one_or_none()

    def get_or_create(self, name: str) -> Author:
        """Return existing Author or create a new one.

        :param name: author name in 'Last, First' form
        :return: existing or newly created Author
        """
        existing = self.get_by_name(name)
        if existing is not None:
            return existing
        author = Author(author=name)
        self.session.add(author)
        self.session.flush()
        return author

    def link_to_paper(self, author_id: int, paper_id: int) -> None:
        """Create an entry in authors_papers linking author to paper.

        :param author_id: authors_id.id value
        :param paper_id: papers.id value
        """
        link = AuthorPaper(author_id=author_id, paper_id=paper_id)
        self.session.add(link)
        self.session.flush()

    def get_names_for_paper(self, paper_id: int) -> list[str]:
        """Return list of author names for the given paper.

        :param paper_id: papers.id value
        :return: list of author name strings in 'Last, First' form
        """
        stmt = (
            select(Author.author)
            .join(AuthorPaper, AuthorPaper.author_id == Author.id)
            .where(AuthorPaper.paper_id == paper_id)
        )
        rows = self.session.execute(stmt).all()
        return [r[0] for r in rows if r[0] is not None]

    def delete_links_for_paper(self, paper_id: int) -> None:
        """Delete all authors_papers rows for the given paper.

        :param paper_id: papers.id value
        """
        stmt = select(AuthorPaper).where(AuthorPaper.paper_id == paper_id)
        links = self.session.execute(stmt).scalars().all()
        for link in links:
            self.session.delete(link)

    def delete_orphans(self) -> int:
        """Delete authors that have no remaining entries in authors_papers.

        :return: number of author rows deleted
        """
        linked_ids_stmt = select(AuthorPaper.author_id).distinct()
        linked_ids = [r[0] for r in self.session.execute(linked_ids_stmt).all()]
        stmt = select(Author).where(Author.id.not_in(linked_ids))
        orphans = self.session.execute(stmt).scalars().all()
        count = len(orphans)
        for orphan in orphans:
            self.session.delete(orphan)
        return count

    def update_name(self, old_name: str, new_name: str) -> Author:
        """Rename an author, merging if new_name already exists.

        If new_name already exists as a different Author row, all papers
        linked to the old author are re-linked to the existing new-name row
        and the old row is deleted (deduplication). If new_name does not
        exist, the Author.author field is updated in place.

        :param old_name: current author name
        :param new_name: desired author name
        :return: the Author row bearing new_name
        :raises KeyError: if old_name is not found
        """
        old_author = self.get_by_name(old_name)
        if old_author is None:
            raise KeyError(f"Author '{old_name}' not found.")
        existing_new = self.get_by_name(new_name)
        if existing_new is not None:
            # Merge: re-link old author's papers to the existing new-name row
            old_id = old_author.id
            new_id = existing_new.id
            links_stmt = select(AuthorPaper).where(AuthorPaper.author_id == old_id)
            old_links = self.session.execute(links_stmt).scalars().all()
            for link in old_links:
                link.author_id = new_id
            self.session.flush()
            # Remove duplicates that may arise from the merge
            all_links_stmt = select(AuthorPaper).where(AuthorPaper.author_id == new_id)
            all_links = self.session.execute(all_links_stmt).scalars().all()
            seen: set[int | None] = set()
            for link in all_links:
                if link.paper_id in seen:
                    self.session.delete(link)
                else:
                    seen.add(link.paper_id)
            self.session.flush()
            self.session.delete(old_author)
            self.session.flush()
            return existing_new
        else:
            old_author.author = new_name
            self.session.flush()
            return old_author


# ---------------------------------------------------------------------------
# PaperRepository
# ---------------------------------------------------------------------------


class PaperRepository:
    """Persistence operations for the papers table.

    Composes AuthorRepository and BibRepository for related entities.

    :param session: active SQLAlchemy session (injected by caller)
    """

    def __init__(self, session: Session) -> None:
        """Initialise with the given session.

        :param session: SQLAlchemy Session to use for all operations
        """
        self.session = session
        self._authors = AuthorRepository(session)
        self._bibs = BibRepository(session)

    def _to_summary(self, paper: Paper) -> PaperSummary:
        """Convert a Paper ORM row to a PaperSummary DTO.

        :param paper: ORM Paper row
        :return: PaperSummary DTO with authors and bibtex resolved
        """
        authors = self._authors.get_names_for_paper(paper.id)
        bib_entry = self._bibs.get_by_id(paper.bibtex_id or "") if paper.bibtex_id else None
        bibtex = bib_entry.bibtex if bib_entry and bib_entry.bibtex else ""
        return PaperSummary(
            id=paper.id,
            title=paper.title or "",
            contents=paper.contents or "",
            bibtex_id=paper.bibtex_id or "",
            authors=authors,
            bibtex=bibtex,
        )

    def search_by_title(self, title: str) -> list[PaperSummary]:
        """Search papers by title (case-insensitive substring match).

        :param title: search term
        :return: list of matching PaperSummary DTOs
        """
        stmt = select(Paper).where(func.lower(Paper.title).contains(title.lower()))
        rows = self.session.execute(stmt).scalars().all()
        return [self._to_summary(p) for p in rows]

    def search_by_author(self, author_name: str) -> list[PaperSummary]:
        """Search papers by author name (case-insensitive substring match).

        :param author_name: author name fragment
        :return: list of matching PaperSummary DTOs
        :raises KeyError: if no author matching the name is found
        """
        author_stmt = select(Author).where(
            func.lower(Author.author).contains(author_name.lower())
        )
        authors = self.session.execute(author_stmt).scalars().all()
        if not authors:
            raise KeyError(f"No author found matching '{author_name}'.")
        paper_ids_set: set[int] = set()
        for author in authors:
            links_stmt = select(AuthorPaper).where(AuthorPaper.author_id == author.id)
            links = self.session.execute(links_stmt).scalars().all()
            for link in links:
                if link.paper_id is not None:
                    paper_ids_set.add(link.paper_id)
        if not paper_ids_set:
            return []
        paper_stmt = select(Paper).where(Paper.id.in_(list(paper_ids_set)))
        rows = self.session.execute(paper_stmt).scalars().all()
        return [self._to_summary(p) for p in rows]

    def get_by_bibtex_id(self, bibtex_id: str) -> PaperSummary | None:
        """Fetch a paper by its BibTeX citation key.

        :param bibtex_id: BibTeX citation key
        :return: PaperSummary DTO, or None if not found
        """
        stmt = select(Paper).where(Paper.bibtex_id == bibtex_id)
        paper = self.session.execute(stmt).scalar_one_or_none()
        if paper is None:
            return None
        return self._to_summary(paper)

    def create(self, data: PaperCreate) -> PaperSummary:
        """Insert a new paper with its BibTeX entry and authors.

        :param data: PaperCreate DTO
        :return: PaperSummary DTO for the newly created paper
        :raises ValueError: if a paper with the same bibtex_id already exists
        """
        existing = self.get_by_bibtex_id(data.bibtex_id)
        if existing is not None:
            raise ValueError(
                f"A paper with bibtex_id '{data.bibtex_id}' already exists."
            )
        bib = self._bibs.get_or_create(data.bibtex_id, data.bibtex)
        paper = Paper(
            title=data.title,
            contents=data.contents,
            bibtex_id=bib.bibtex_id,
        )
        self.session.add(paper)
        self.session.flush()
        for author_name in data.authors:
            author = self._authors.get_or_create(author_name)
            self._authors.link_to_paper(author.id, paper.id)
        return self._to_summary(paper)

    def update_title(self, bibtex_id: str, new_title: str) -> PaperSummary:
        """Update the title of a paper identified by bibtex_id.

        :param bibtex_id: BibTeX citation key
        :param new_title: new title string
        :return: updated PaperSummary DTO
        :raises KeyError: if no paper found for the given bibtex_id
        """
        stmt = select(Paper).where(Paper.bibtex_id == bibtex_id)
        paper = self.session.execute(stmt).scalar_one_or_none()
        if paper is None:
            raise KeyError(f"Paper '{bibtex_id}' not found.")
        paper.title = new_title
        self.session.flush()
        return self._to_summary(paper)

    def update_contents(self, bibtex_id: str, new_contents: str) -> PaperSummary:
        """Update the contents/summary of a paper identified by bibtex_id.

        :param bibtex_id: BibTeX citation key
        :param new_contents: new summary string
        :return: updated PaperSummary DTO
        :raises KeyError: if no paper found for the given bibtex_id
        """
        stmt = select(Paper).where(Paper.bibtex_id == bibtex_id)
        paper = self.session.execute(stmt).scalar_one_or_none()
        if paper is None:
            raise KeyError(f"Paper '{bibtex_id}' not found.")
        paper.contents = new_contents
        self.session.flush()
        return self._to_summary(paper)

    def delete(self, bibtex_id: str) -> str:
        """Delete a paper and its authorship links by bibtex_id.

        Orphan authors (with no remaining papers) are deleted automatically.
        The BibTeX entry in the bib table is also deleted.

        :param bibtex_id: BibTeX citation key
        :return: title of the deleted paper
        :raises KeyError: if no paper found for the given bibtex_id
        """
        stmt = select(Paper).where(Paper.bibtex_id == bibtex_id)
        paper = self.session.execute(stmt).scalar_one_or_none()
        if paper is None:
            raise KeyError(f"Paper '{bibtex_id}' not found.")
        title = paper.title or ""
        paper_id = paper.id
        self._authors.delete_links_for_paper(paper_id)
        self._authors.delete_orphans()
        bib_entry = self._bibs.get_by_id(bibtex_id)
        if bib_entry is not None:
            self.session.delete(bib_entry)
        self.session.delete(paper)
        self.session.flush()
        return title
