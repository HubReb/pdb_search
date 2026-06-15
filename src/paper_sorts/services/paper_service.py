"""Domain operations for the paper database.

The service layer orchestrates the repositories and exposes pydantic DTOs. It contains no SQL,
no ``rich``, and no I/O — per the constitution's layering rule, database driver / ORM imports
stay in ``db/``. Each operation opens a session through ``with_session`` so writes commit on
success and roll back on failure.
"""

from __future__ import annotations

from typing import Literal, assert_never

from sqlalchemy import Engine

from paper_sorts.db.repositories import (
    AuthorRepository,
    BibRepository,
    PaperCreate,
    PaperRepository,
    PaperSummary,
)
from paper_sorts.db.session import with_session

UpdatableTable = Literal["papers", "bib", "authors_id"]


class PaperService:
    """High-level operations over the paper database.

    :ivar engine: the SQLAlchemy engine all sessions are bound to.
    """

    def __init__(self, engine: Engine) -> None:
        """Bind the service to an engine.

        :param engine: the engine sessions are opened against.
        """
        self.engine = engine

    def search_by_title(self, title: str) -> list[PaperSummary]:
        """Search for papers by exact title.

        :param title: the exact title to search for.
        :return: matching summaries (possibly empty).
        """
        with with_session(self.engine) as session:
            return PaperRepository(session).search_by_title(title)

    def search_by_author(self, author: str) -> list[PaperSummary]:
        """Search for papers by exact author name.

        :param author: the exact author name (``"Last, First"``).
        :return: matching summaries (possibly empty).
        """
        with with_session(self.engine) as session:
            return PaperRepository(session).search_by_author(author)

    def add_paper(self, paper: PaperCreate) -> None:
        """Add a paper, its BibTeX entry, and its author links.

        :param paper: the paper to create.
        :raises ValueError: if the BibTeX key already exists.
        """
        with with_session(self.engine) as session:
            bibs = BibRepository(session)
            if bibs.exists(paper.bibtex_id):
                raise ValueError(f"Entry {paper.bibtex_id} already exists")
            bibs.add(paper.bibtex_id, paper.bibtex)
            papers = PaperRepository(session)
            paper_id = papers.add_paper_row(paper.title, paper.summary, paper.bibtex_id)
            authors = AuthorRepository(session)
            for author in paper.authors:
                author_id = authors.get_or_create_author_id(author)
                authors.link(author_id, paper_id)

    def update_field(
        self,
        table: UpdatableTable,
        column: str,
        value: str,
        identifier: str,
    ) -> None:
        """Update a single editable field, dispatching on the target table.

        The ``authors_papers`` link table is intentionally not updatable. The dispatch is
        exhaustive over :data:`UpdatableTable` via ``assert_never``.

        :param table: the table to update (``"papers"``, ``"bib"``, or ``"authors_id"``).
        :param column: the column to update.
        :param value: the new value.
        :param identifier: the row identifier (paper id, bibtex key, or author name).
        :raises ValueError: if the column is not editable in the chosen table.
        """
        with with_session(self.engine) as session:
            match table:
                case "papers":
                    PaperRepository(session).update_papers_column(int(identifier), column, value)
                case "bib":
                    if column != "bibtex":
                        raise ValueError(f"Column {column} is not present in table bib")
                    BibRepository(session).update_bibtex(identifier, value)
                case "authors_id":
                    if column != "author":
                        raise ValueError(f"Column {column} is not present in table authors_id")
                    AuthorRepository(session).update_author_name(identifier, value)
                case _:
                    assert_never(table)

    def delete_paper(self, paper_id: int) -> bool:
        """Delete a paper, its BibTeX entry, and its author links.

        Authors left with no remaining papers are removed (orphan cleanup).

        :param paper_id: the ``papers.id`` to delete.
        :return: ``True`` if the paper existed and was deleted, ``False`` otherwise.
        """
        with with_session(self.engine) as session:
            papers = PaperRepository(session)
            summary = papers.get_by_id(paper_id)
            if summary is None:
                return False
            authors = AuthorRepository(session)
            for author in summary.authors.split(" and "):
                if author:
                    authors.unlink_for_paper(author, paper_id)
            papers.delete_paper(paper_id)
            if summary.bibtex_id:
                BibRepository(session).delete(summary.bibtex_id)
            return True
