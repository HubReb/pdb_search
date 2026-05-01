"""Paper service — domain operations driving the CLI.

Per constitution Principle I (v1.3.0), the service layer never imports
sqlalchemy at runtime. It composes the repository classes exposed by
:mod:`paper_sorts.db.repositories`. The :class:`Session` annotation on the
constructor is guarded by ``TYPE_CHECKING`` so the only sqlalchemy mention
in this file is at type-check time, never at runtime.

Most methods are thin pass-throughs to the repository surface. The
non-trivial piece is :meth:`PaperService.update_field`, which preserves
the table x field grid from the legacy ``UserInteraction.update`` dialog.
Updating ``papers.id``, ``papers.bibtex_id``, ``bib.bibtex_id``, or
``authors_id.id`` is rejected with a plain-language ``ValueError``; the
BibTeX identifier itself is explicitly listed because the legacy dialog
had a special case forbidding it (only the *source* string under
``bib.bibtex`` is editable).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, assert_never, cast

from paper_sorts.db.repositories import (
    AuthorRepository,
    BibRepository,
    DuplicateBibtexIdError,
    PaperCreate,
    PaperRepository,
    PaperSummary,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class PaperService:
    """Domain operations for the modernized CLI.

    The service binds to a single SQLAlchemy session held by the caller's
    :func:`paper_sorts.db.session.with_session` boundary; every method runs
    inside that transaction.
    """

    def __init__(self, session: Session) -> None:
        """Construct the three repositories that back this service."""
        self._papers = PaperRepository(session)
        self._bibs = BibRepository(session)
        self._authors = AuthorRepository(session)

    def find_by_id(self, paper_id: int) -> PaperSummary | None:
        """Return the paper with that id or ``None`` if no such row exists."""
        return self._papers.find_by_id(paper_id)

    def search_by_title(self, title: str) -> list[PaperSummary]:
        """Return papers whose title matches ``title`` exactly."""
        return self._papers.find_by_title(title)

    def search_by_author(self, name: str) -> list[PaperSummary]:
        """Return papers credited to ``name`` (exact match)."""
        return self._papers.find_by_author(name)

    def add_paper(self, payload: PaperCreate) -> PaperSummary:
        """Insert a paper, its bib row, and its authors and links atomically.

        Raises:
            DuplicateBibtexIdError: When ``payload.bibtex_id`` is already
                in the ``bib`` table. The check runs *before* any insert
                so the CLI can render a plain-language error and the
                database state is unchanged (per the FR-002 contract).
        """
        if self._bibs.exists(payload.bibtex_id):
            msg = f"BibTeX key {payload.bibtex_id!r} already exists in the database."
            raise DuplicateBibtexIdError(msg)
        return self._papers.add(payload)

    def update_field(
        self,
        table: Literal["papers", "bib", "authors"],
        field: str,
        identifier: int | str,
        value: str,
    ) -> None:
        """Update a single editable field, dispatching by table.

        Raises:
            ValueError: If ``field`` is not editable on ``table`` (which
                includes the explicit "the BibTeX identifier itself is
                not editable" rule), or if ``value`` is empty.
            TypeError: If ``identifier`` does not match the table's id
                type (``int`` for papers/authors, ``str`` for bib).
        """
        if not value:
            msg = "New value cannot be empty."
            raise ValueError(msg)

        match table:
            case "papers":
                if field not in {"title", "contents"}:
                    msg = f"papers.{field!r} is not editable. Editable fields: title, contents."
                    raise ValueError(msg)
                if not isinstance(identifier, int):
                    msg = "Identifier for table 'papers' must be an int (paper id)."
                    raise TypeError(msg)
                if self._papers.find_by_id(identifier) is None:
                    msg = f"No paper with id {identifier}."
                    raise ValueError(msg)
                self._papers.update_field(
                    identifier,
                    cast("Literal['title', 'contents']", field),
                    value,
                )
            case "bib":
                if field == "bibtex_id":
                    msg = "The BibTeX identifier itself is not editable; only its source string is."
                    raise ValueError(msg)
                if field != "bibtex":
                    msg = f"bib.{field!r} is not editable. Editable field: bibtex."
                    raise ValueError(msg)
                if not isinstance(identifier, str):
                    msg = "Identifier for table 'bib' must be a str (bibtex_id)."
                    raise TypeError(msg)
                self._bibs.update(identifier, value)
            case "authors":
                if field != "author":
                    msg = f"authors.{field!r} is not editable. Editable field: author."
                    raise ValueError(msg)
                if not isinstance(identifier, int):
                    msg = "Identifier for table 'authors' must be an int (author id)."
                    raise TypeError(msg)
                self._authors.update_name(identifier, value)
            case _:
                assert_never(table)

    def delete_paper(self, paper_id: int) -> None:
        """Delete a paper, cascade its links, drop orphan authors and bib row.

        Raises:
            ValueError: If no paper with ``paper_id`` exists. Caught by the
                CLI delete command and rendered as a plain-language error.
        """
        if self._papers.find_by_id(paper_id) is None:
            msg = f"No paper with id {paper_id}."
            raise ValueError(msg)
        self._papers.delete(paper_id)
