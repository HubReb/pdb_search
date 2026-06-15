"""Domain operations — pure orchestration over DTOs and repositories.

No SQL, no rich, no I/O lives here (constitution Principle I / FR-014); this
layer wires the CLI to the persistence repositories via DTOs only. ORM types
never appear in this module.
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


def search_by_title(engine: Engine, title: str) -> list[PaperSummary]:
    """Search for papers by exact title.

    :param engine: the database engine.
    :param title: the title to match.
    :returns: matching paper summaries (empty if none).
    """
    with with_session(engine) as session:
        return PaperRepository(session).search_by_title(title)


def search_by_author(engine: Engine, author: str) -> list[PaperSummary]:
    """Search for papers by exact author name.

    :param engine: the database engine.
    :param author: the author name to match.
    :returns: matching paper summaries (empty if none).
    """
    with with_session(engine) as session:
        return PaperRepository(session).search_by_author(author)


def add_paper(engine: Engine, paper: PaperCreate) -> int:
    """Add a new paper.

    :param engine: the database engine.
    :param paper: the paper to create.
    :returns: the new internal ``papers.id``.
    :raises ValueError: if the BibTeX key already exists.
    """
    with with_session(engine) as session:
        return PaperRepository(session).add(paper)


def delete_paper(engine: Engine, paper_id: int) -> None:
    """Delete a paper and its bib, links, and orphaned authors.

    :param engine: the database engine.
    :param paper_id: the internal ``papers.id`` to delete.
    """
    with with_session(engine) as session:
        PaperRepository(session).delete(paper_id)


def update_field(
    engine: Engine,
    table: UpdatableTable,
    column: str,
    identifier: str,
    value: str,
) -> None:
    """Update a single editable field, with compile-time table exhaustiveness.

    IDs are immutable and the ``authors_papers`` link table is not updatable;
    both are rejected before persistence (legacy ``update_entry`` semantics).
    The ``match`` over the ``Literal`` table set ends in ``assert_never`` so
    mypy proves the cases are exhaustive.

    :param engine: the database engine.
    :param table: which table to update — ``papers``/``bib``/``authors_id``.
    :param column: the column to set (validated per table).
    :param identifier: the row key (paper id, bibtex key, or author name).
    :param value: the new value.
    :raises ValueError: for an ID column, an unknown column, or an unsupported
        table such as ``authors_papers``.
    """
    if "_id" in column:
        raise ValueError("IDs are unique and must not be changed!")
    with with_session(engine) as session:
        match table:
            case "papers":
                _update_paper(PaperRepository(session), column, identifier, value)
            case "bib":
                _update_bib(BibRepository(session), column, identifier, value)
            case "authors_id":
                _update_author(AuthorRepository(session), column, identifier, value)
            case _ as unreachable:
                assert_never(unreachable)


def _update_paper(repo: PaperRepository, column: str, identifier: str, value: str) -> None:
    """Update an editable ``papers`` column (``title`` or ``contents``).

    :param repo: the paper repository.
    :param column: ``title`` or ``contents``.
    :param identifier: the ``papers.id`` as a string.
    :param value: the new value.
    :raises ValueError: if the column is not editable or the id is malformed.
    """
    try:
        paper_id = int(identifier)
    except ValueError as exc:
        raise ValueError(f"Paper identifier {identifier!r} is not a numeric id") from exc
    repo.update_column(paper_id, column, value)


def _update_bib(repo: BibRepository, column: str, identifier: str, value: str) -> None:
    """Update the ``bib.bibtex`` source (the only editable bib column).

    :param repo: the bib repository.
    :param column: must be ``bibtex``.
    :param identifier: the BibTeX key.
    :param value: the new BibTeX source.
    :raises ValueError: if the column is not ``bibtex``.
    """
    if column != "bibtex":
        raise ValueError(f"Column {column} is not present in table bibtex")
    repo.update_bibtex(identifier, value)


def _update_author(repo: AuthorRepository, column: str, identifier: str, value: str) -> None:
    """Rename an author (the only editable ``authors_id`` column).

    :param repo: the author repository.
    :param column: must be ``author``.
    :param identifier: the current author name.
    :param value: the new author name.
    :raises ValueError: if the column is not ``author``.
    """
    if column != "author":
        raise ValueError(f"Column {column} is not present in table authors_id")
    repo.rename(identifier, value)
