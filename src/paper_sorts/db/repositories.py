"""Repositories and DTOs — the boundary between services and the ORM.

Services depend only on the pydantic DTOs and the repository methods defined
here; they never import ORM types (constitution Principle I). Each repository
takes an open :class:`~sqlalchemy.orm.Session`; the caller owns the session
lifecycle via :func:`paper_sorts.db.session.with_session`.

The repositories reproduce the legacy ``DatabaseConnector`` semantics — author
reuse-by-name, orphan-author cleanup on delete/rename, the authors-joined-with-
``" and "`` display string, and the ``bib.bibtex`` UNIQUE guard on update.
"""

from __future__ import annotations

from pydantic import BaseModel
from sqlalchemy import delete, select, update
from sqlalchemy.orm import Session

from paper_sorts.db.models import Author, AuthorPaper, Bib, Paper


class PaperSummary(BaseModel):
    """Read model returned by search operations (legacy pretty-print shape).

    :ivar paper_id: internal ``papers.id``.
    :ivar title: publication title.
    :ivar authors: author names joined with ``" and "``.
    :ivar summary: the paper's one-sentence summary (``papers.contents``).
    :ivar bibtex_id: the BibTeX key.
    :ivar bibtex: the full BibTeX source string.
    """

    paper_id: int
    title: str
    authors: str
    summary: str
    bibtex_id: str
    bibtex: str


class PaperCreate(BaseModel):
    """Write model consumed by add / bulk-import.

    :ivar title: publication title.
    :ivar summary: one-sentence summary.
    :ivar authors: author names, each in ``"Last, First"`` form.
    :ivar bibtex_id: unique BibTeX key.
    :ivar bibtex: full BibTeX source string.
    """

    title: str
    summary: str
    authors: list[str]
    bibtex_id: str
    bibtex: str


def _join_authors(names: list[str]) -> str:
    """Join author names with ``" and "`` (legacy display string).

    :param names: ordered author names.
    :returns: the joined display string.
    """
    return " and ".join(names)


class PaperRepository:
    """CRUD and search over papers and their author/bib relations."""

    def __init__(self, session: Session) -> None:
        """Bind the repository to an open session.

        :param session: the active SQLAlchemy session.
        """
        self.session = session

    def _summaries_for(self, papers: list[Paper]) -> list[PaperSummary]:
        """Build :class:`PaperSummary` rows for the given papers.

        :param papers: ORM ``Paper`` rows.
        :returns: one summary per paper, with authors collapsed and bib joined.
        """
        summaries: list[PaperSummary] = []
        for paper in papers:
            author_names = self._authors_for_paper(paper.id)
            bibtex = paper.bib.bibtex if paper.bib and paper.bib.bibtex else ""
            summaries.append(
                PaperSummary(
                    paper_id=paper.id,
                    title=paper.title or "",
                    authors=_join_authors(author_names),
                    summary=paper.contents or "",
                    bibtex_id=paper.bibtex_id or "",
                    bibtex=bibtex,
                )
            )
        return summaries

    def _authors_for_paper(self, paper_id: int) -> list[str]:
        """Return ordered author names for a paper via the link table.

        :param paper_id: ``papers.id``.
        :returns: author names in link-row order.
        """
        stmt = (
            select(Author.author)
            .join(AuthorPaper, AuthorPaper.author_id == Author.id)
            .where(AuthorPaper.paper_id == paper_id)
            .order_by(AuthorPaper.id)
        )
        return [name for name in self.session.scalars(stmt).all() if name]

    def search_by_title(self, title: str) -> list[PaperSummary]:
        """Find papers by exact title.

        :param title: the title to match exactly.
        :returns: one summary per matching paper (empty if none).
        """
        papers = list(self.session.scalars(select(Paper).where(Paper.title == title)).all())
        return self._summaries_for(papers)

    def search_by_author(self, author: str) -> list[PaperSummary]:
        """Find papers an author is credited on.

        :param author: the author name to match exactly.
        :returns: one summary per matching paper (empty if none).
        """
        stmt = (
            select(Paper)
            .join(AuthorPaper, AuthorPaper.paper_id == Paper.id)
            .join(Author, Author.id == AuthorPaper.author_id)
            .where(Author.author == author)
            .order_by(Paper.id)
        )
        papers = list(self.session.scalars(stmt).all())
        return self._summaries_for(papers)

    def get_by_id(self, paper_id: int) -> PaperSummary | None:
        """Fetch a single paper summary by internal id.

        :param paper_id: ``papers.id``.
        :returns: the summary, or ``None`` if absent.
        """
        paper = self.session.get(Paper, paper_id)
        if paper is None:
            return None
        return self._summaries_for([paper])[0]

    def _link_author(self, name: str, paper_id: int) -> None:
        """Link an author to a paper, reusing an existing author row by name.

        :param name: author name in ``"Last, First"`` form.
        :param paper_id: ``papers.id`` to link to.
        """
        author = self.session.scalars(select(Author).where(Author.author == name)).first()
        if author is None:
            author = Author(author=name)
            self.session.add(author)
            self.session.flush()
        self.session.add(AuthorPaper(author_id=author.id, paper_id=paper_id))

    def add(self, paper: PaperCreate) -> int:
        """Insert a paper, its bib entry, and its author links.

        :param paper: the paper to create.
        :returns: the new ``papers.id``.
        :raises ValueError: if the BibTeX key already exists.
        """
        exists = self.session.get(Bib, paper.bibtex_id)
        if exists is not None:
            raise ValueError(f"Entry {paper.bibtex_id} already exists")
        self.session.add(Bib(bibtex_id=paper.bibtex_id, bibtex=paper.bibtex))
        new_paper = Paper(title=paper.title, contents=paper.summary, bibtex_id=paper.bibtex_id)
        self.session.add(new_paper)
        self.session.flush()
        for name in paper.authors:
            self._link_author(name, new_paper.id)
        return new_paper.id

    def delete(self, paper_id: int) -> None:
        """Delete a paper and its bib, links, and now-orphaned authors.

        :param paper_id: ``papers.id`` to remove.
        """
        paper = self.session.get(Paper, paper_id)
        if paper is None:
            return
        author_ids = [
            link.author_id
            for link in self.session.scalars(
                select(AuthorPaper).where(AuthorPaper.paper_id == paper_id)
            ).all()
            if link.author_id is not None
        ]
        self.session.execute(delete(AuthorPaper).where(AuthorPaper.paper_id == paper_id))
        bibtex_id = paper.bibtex_id
        self.session.delete(paper)
        self.session.flush()
        if bibtex_id is not None:
            bib = self.session.get(Bib, bibtex_id)
            if bib is not None:
                self.session.delete(bib)
        for author_id in set(author_ids):
            _delete_author_if_orphan(self.session, author_id)


class AuthorRepository:
    """Author-name updates with link repointing and orphan cleanup."""

    def __init__(self, session: Session) -> None:
        """Bind the repository to an open session.

        :param session: the active SQLAlchemy session.
        """
        self.session = session

    def rename(self, old_name: str, new_name: str) -> None:
        """Rename an author, repointing links and cleaning up duplicates.

        Mirrors legacy ``__update_author``: if ``new_name`` already exists, the
        old author's links are repointed to it and duplicate links removed;
        otherwise a new author row receives the links. The old author row is
        deleted, and any author left with no links is removed.

        :param old_name: current author name.
        :param new_name: desired author name.
        :raises ValueError: if the old author does not exist.
        """
        old = self.session.scalars(select(Author).where(Author.author == old_name)).first()
        if old is None:
            raise ValueError(f"Author {old_name} not found")
        existing = self.session.scalars(select(Author).where(Author.author == new_name)).first()
        if existing is not None:
            target_id = existing.id
            for link in self.session.scalars(
                select(AuthorPaper).where(AuthorPaper.author_id == old.id)
            ).all():
                duplicate = self.session.scalars(
                    select(AuthorPaper).where(
                        AuthorPaper.author_id == target_id,
                        AuthorPaper.paper_id == link.paper_id,
                    )
                ).first()
                if duplicate is not None:
                    self.session.delete(link)
                else:
                    link.author_id = target_id
        else:
            new_author = Author(author=new_name)
            self.session.add(new_author)
            self.session.flush()
            target_id = new_author.id
            self.session.execute(
                update(AuthorPaper)
                .where(AuthorPaper.author_id == old.id)
                .values(author_id=target_id)
            )
        self.session.delete(old)
        self.session.flush()
        _delete_author_if_orphan(self.session, target_id)


class BibRepository:
    """Updates to BibTeX source strings."""

    def __init__(self, session: Session) -> None:
        """Bind the repository to an open session.

        :param session: the active SQLAlchemy session.
        """
        self.session = session

    def update_bibtex(self, bibtex_id: str, new_source: str) -> None:
        """Update a bib entry's source, enforcing the UNIQUE constraint.

        :param bibtex_id: the BibTeX key identifying the row.
        :param new_source: the new BibTeX source string.
        :raises ValueError: if ``new_source`` already exists (legacy behaviour),
            or if ``bibtex_id`` is unknown.
        """
        clash = self.session.scalars(select(Bib).where(Bib.bibtex == new_source)).first()
        if clash is not None:
            raise ValueError("bibtex is unique - value already exists!")
        bib = self.session.get(Bib, bibtex_id)
        if bib is None:
            raise ValueError(f"bibtex_id {bibtex_id} not found")
        bib.bibtex = new_source


def _delete_author_if_orphan(session: Session, author_id: int) -> None:
    """Delete an author row that no longer links to any paper.

    :param session: the active session.
    :param author_id: ``authors_id.id`` to check.
    """
    remaining = session.scalars(
        select(AuthorPaper).where(AuthorPaper.author_id == author_id)
    ).first()
    if remaining is None:
        author = session.get(Author, author_id)
        if author is not None:
            session.delete(author)
