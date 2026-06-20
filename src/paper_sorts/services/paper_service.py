"""Domain service: high-level paper operations.

Pure orchestration over the repositories — no SQL, no rich, no I/O. Every public
method opens a context-managed session (commit on success, rollback on error).
"""

from __future__ import annotations

from typing import Literal, assert_never

from paper_sorts.db.repositories import (
    AuthorRepository,
    BibRepository,
    PaperCreate,
    PaperRepository,
    PaperSummary,
)
from paper_sorts.db.session import Engine, Session, with_session

UpdatableTable = Literal["papers", "bib", "authors_id"]


class PaperService:
    """Orchestrates search, add, update, and delete over the repositories."""

    def __init__(self, engine: Engine) -> None:
        """Bind the service to an engine.

        :param engine: the SQLAlchemy engine to open sessions against.
        """
        self._engine = engine
        self._papers = PaperRepository()
        self._authors = AuthorRepository()
        self._bib = BibRepository()

    def search_by_title(self, title: str) -> list[PaperSummary]:
        """Return all papers exactly matching ``title``."""
        with with_session(self._engine) as session:
            return self._papers.get_by_title(session, title)

    def search_by_author(self, author: str) -> list[PaperSummary]:
        """Return all papers linked to the author named ``author``."""
        with with_session(self._engine) as session:
            return self._authors.get_papers_by_author(session, author)

    def add_paper(self, paper: PaperCreate) -> None:
        """Persist a paper: bib row, paper row, then author links.

        A duplicate BibTeX key is rejected. Any failure rolls the whole unit of
        work back (the surrounding session), so a partial add never persists.

        :param paper: the paper to add (≥1 author).
        :raises ValueError: if the BibTeX key already exists.
        """
        with with_session(self._engine) as session:
            if self._bib.exists(session, paper.bibtex_id) or self._papers.exists_bibtex_id(
                session, paper.bibtex_id
            ):
                raise ValueError(f"Entry {paper.bibtex_id} already exists")
            self._bib.add(session, paper.bibtex_id, paper.bibtex)
            paper_id = self._papers.add(session, paper)
            for author in paper.authors:
                self._authors.link(session, author, paper_id)

    def update_field(
        self,
        table: UpdatableTable,
        column: str,
        identifier: str,
        new_value: str,
    ) -> None:
        """Update one editable field in one table.

        :param table: which table — ``papers``, ``bib``, or ``authors_id``.
        :param column: the column to change.
        :param identifier: the row key (paper id, bibtex key, or author id).
        :param new_value: the new value.
        :raises ValueError: if the column is an id, or not editable for the table.
        """
        if column.endswith("_id"):
            raise ValueError("IDs are unique and must not be changed!")
        with with_session(self._engine) as session:
            match table:
                case "papers":
                    self._update_papers(session, column, int(identifier), new_value)
                case "bib":
                    if column != "bibtex":
                        raise ValueError(f"Column {column} is not present in table bib")
                    self._bib.update(session, identifier, new_value)
                case "authors_id":
                    if column != "author":
                        raise ValueError(f"Column {column} is not present in table authors_id")
                    self._authors.rename(session, int(identifier), new_value)
                case _:
                    assert_never(table)

    def _update_papers(self, session: Session, column: str, paper_id: int, value: str) -> None:
        match column:
            case "title":
                self._papers.update_title(session, paper_id, value)
            case "contents":
                self._papers.update_contents(session, paper_id, value)
            case _:
                raise ValueError(f"Column {column} is not present in table papers")

    def delete_paper(self, paper_id: int) -> None:
        """Delete a paper, its author links (and orphan authors), and its bib row.

        :param paper_id: the internal id of the paper to remove.
        :raises ValueError: if no paper with that id exists.
        """
        with with_session(self._engine) as session:
            summary = self._papers.get_by_id(session, paper_id)
            if summary is None:
                raise ValueError(f"No paper with id {paper_id}")
            self._authors.unlink_all_for_paper(session, paper_id)
            self._papers.delete(session, paper_id)
            if summary.bibtex_id:
                self._bib.delete(session, summary.bibtex_id)
