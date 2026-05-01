"""Repository surface — every SQLAlchemy query in the application lives here.

Per constitution Principle I (v1.3.0), this module plus :mod:`db.session` and
:mod:`db.models` are the only files permitted to import ``sqlalchemy``. The
service layer interacts with the persistence layer exclusively through these
repository classes and the colocated pydantic models :class:`PaperSummary`
and :class:`PaperCreate`.

Two design notes worth flagging for reviewers:

* :meth:`PaperRepository.add` and :meth:`PaperRepository.delete` are
  symmetric — both encapsulate the full multi-step write so the service
  layer remains free of cross-table orchestration. The caller's
  :func:`paper_sorts.db.session.with_session` boundary supplies atomicity.
* :meth:`PaperRepository.delete` removes the bib row only when no other
  paper still references its ``bibtex_id``. This is *narrower* than the
  legacy ``DatabaseConnector.delete_paper_entry_from_database`` which
  unconditionally dropped the bib row; the narrowing is a documented
  refinement (see ``data-model.md`` invariant table).
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import delete, select, update
from sqlalchemy.orm import Session

from paper_sorts.db.models import Author, Authorship, BibEntry, Paper


class PaperSummary(BaseModel):
    """Display projection of a paper with its authors and bib source."""

    model_config = ConfigDict(frozen=True)

    id: int
    title: str
    contents: str
    bibtex_id: str
    bibtex: str | None
    authors: tuple[str, ...]


class PaperCreate(BaseModel):
    """Input record for :meth:`PaperRepository.add`."""

    model_config = ConfigDict(frozen=True)

    title: str = Field(min_length=1)
    contents: str = Field(min_length=1)
    bibtex_id: str = Field(min_length=1)
    bibtex: str = Field(min_length=1)
    authors: tuple[str, ...] = Field(min_length=1)


class AuthorRepository:
    """Reads and writes against the ``authors_id`` table."""

    def __init__(self, session: Session) -> None:
        """Bind to a unit-of-work session yielded by :func:`with_session`."""
        self._session = session

    def upsert(self, name: str) -> Author:
        """Return the author row for ``name``, creating it if absent.

        The lookup is by exact-string match on the ``author`` column, which
        matches the legacy de-duplication semantics. Two authors with
        identical names are treated as the same author (a documented
        limitation, not a bug — spec edge case).
        """
        stmt = select(Author).where(Author.name == name)
        existing = self._session.execute(stmt).scalar_one_or_none()
        if existing is not None:
            return existing
        author = Author(name=name)
        self._session.add(author)
        self._session.flush()
        return author

    def update_name(self, author_id: int, name: str) -> None:
        """Rename an author by id."""
        stmt = update(Author).where(Author.id == author_id).values(name=name)
        self._session.execute(stmt)


class DuplicateBibtexIdError(ValueError):
    """Raised by :meth:`PaperService.add_paper` when ``bibtex_id`` already exists.

    Subclasses :class:`ValueError` so callers that catch the broader type
    keep working; specific catches give the CLI a single targeted handler
    for the documented "duplicate BibTeX key" plain-language error.
    """


class BibRepository:
    """Reads and writes against the ``bib`` table."""

    def __init__(self, session: Session) -> None:
        """Bind to a unit-of-work session yielded by :func:`with_session`."""
        self._session = session

    def exists(self, bibtex_id: str) -> bool:
        """Return ``True`` if a bib row with that key is already present."""
        stmt = select(BibEntry.bibtex_id).where(BibEntry.bibtex_id == bibtex_id)
        return self._session.execute(stmt).scalar_one_or_none() is not None

    def add(self, bibtex_id: str, bibtex: str) -> None:
        """Insert a new bib row keyed by ``bibtex_id``."""
        self._session.add(BibEntry(bibtex_id=bibtex_id, bibtex=bibtex))

    def update(self, bibtex_id: str, bibtex: str) -> None:
        """Replace the BibTeX source for an existing bib row."""
        stmt = update(BibEntry).where(BibEntry.bibtex_id == bibtex_id).values(bibtex=bibtex)
        self._session.execute(stmt)


class PaperRepository:
    """Reads and writes against the ``papers`` table and its joins."""

    def __init__(self, session: Session) -> None:
        """Bind to a unit-of-work session yielded by :func:`with_session`."""
        self._session = session
        self._authors = AuthorRepository(session)

    def find_by_title(self, title: str) -> list[PaperSummary]:
        """Return all papers whose title matches ``title`` exactly."""
        stmt = select(Paper).where(Paper.title == title)
        return [self._project(p) for p in self._session.execute(stmt).scalars()]

    def find_by_author(self, author_name: str) -> list[PaperSummary]:
        """Return all papers credited to ``author_name`` (exact match).

        Implemented via a subquery on ``Authorship.paper_id`` so duplicate
        link rows (the documented quirk) do not duplicate the result list.
        """
        linked = (
            select(Authorship.paper_id)
            .join(Author, Author.id == Authorship.author_id)
            .where(Author.name == author_name)
        )
        stmt = select(Paper).where(Paper.id.in_(linked))
        return [self._project(p) for p in self._session.execute(stmt).scalars()]

    def add(self, payload: PaperCreate) -> PaperSummary:
        """Insert ``bib`` + ``papers`` + ``authors_id`` + ``authors_papers`` rows.

        The whole sequence runs inside the caller's
        :func:`with_session` transaction. Any DB error propagates out and
        the unit-of-work rolls back.

        Returns a :class:`PaperSummary` projection of the inserted paper —
        not the ORM :class:`Paper` — so the service layer never crosses
        the sqlalchemy boundary (a deliberate narrowing of the
        ``data-model.md`` sketch's ``-> Paper`` return type).
        """
        self._session.add(BibEntry(bibtex_id=payload.bibtex_id, bibtex=payload.bibtex))

        paper = Paper(
            title=payload.title,
            contents=payload.contents,
            bibtex_id=payload.bibtex_id,
        )
        self._session.add(paper)
        self._session.flush()  # populate paper.id

        for name in payload.authors:
            author = self._authors.upsert(name)
            self._session.add(Authorship(author_id=author.id, paper_id=paper.id))
        self._session.flush()
        return self._project(paper)

    def update_field(
        self,
        paper_id: int,
        field: Literal["title", "contents"],
        value: str,
    ) -> None:
        """Update one of the two mutable scalar fields on ``papers``.

        The ``Literal`` constraint blocks updating ``id`` or ``bibtex_id``
        at the type level — the latter is a documented hard rule (the
        BibTeX identifier itself is not editable; only its source is).
        """
        column = getattr(Paper, field)
        stmt = update(Paper).where(Paper.id == paper_id).values({column: value})
        self._session.execute(stmt)

    def delete(self, paper_id: int) -> None:
        """Delete a paper and its dependents.

        Sequence: collect the paper's author ids and bib id; drop its
        ``authors_papers`` links; drop the paper row; drop any author rows
        whose only links were to this paper (orphans); drop the bib row
        only if no other paper still references it.
        """
        author_ids = list(
            self._session.execute(
                select(Authorship.author_id).where(Authorship.paper_id == paper_id)
            ).scalars()
        )
        bib_id = self._session.execute(
            select(Paper.bibtex_id).where(Paper.id == paper_id)
        ).scalar_one_or_none()

        self._session.execute(delete(Authorship).where(Authorship.paper_id == paper_id))
        self._session.execute(delete(Paper).where(Paper.id == paper_id))
        self._session.flush()

        for aid in author_ids:
            still_linked = self._session.execute(
                select(Authorship.id).where(Authorship.author_id == aid).limit(1)
            ).scalar_one_or_none()
            if still_linked is None:
                self._session.execute(delete(Author).where(Author.id == aid))

        if bib_id is not None:
            still_referenced = self._session.execute(
                select(Paper.id).where(Paper.bibtex_id == bib_id).limit(1)
            ).scalar_one_or_none()
            if still_referenced is None:
                self._session.execute(delete(BibEntry).where(BibEntry.bibtex_id == bib_id))

        self._session.flush()

    def _project(self, paper: Paper) -> PaperSummary:
        """Build a :class:`PaperSummary` from a loaded ORM ``Paper``.

        NULL columns at the schema level are coerced to empty strings so
        the display layer never has to handle ``None`` (the application
        produces non-NULL values in steady state, but the schema permits
        NULL — see ``data-model.md`` invariant table).
        """
        return PaperSummary(
            id=paper.id,
            title=paper.title or "",
            contents=paper.contents or "",
            bibtex_id=paper.bibtex_id or "",
            bibtex=paper.bib_entry.bibtex if paper.bib_entry else None,
            authors=tuple(a.name or "" for a in paper.authors),
        )
