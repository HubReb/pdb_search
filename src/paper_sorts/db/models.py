"""SQLAlchemy 2.x ORM models for paper-sorts.

Defines four tables that mirror the legacy ``DatabaseConnector.create_tables()``
DDL exactly — no schema changes are introduced by this module.

Schema preservation contract (see CLAUDE.md):
- No NOT NULL constraints outside primary keys.
- No DDL foreign keys on ``authors_papers``.
- No indexes beyond existing primary keys.

Only modules under ``src/paper_sorts/db/`` may import SQLAlchemy.
"""

from __future__ import annotations

from sqlalchemy import ForeignKey, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Shared declarative base for all ORM models."""


class Bib(Base):
    """BibTeX entry table.

    Stores the full BibTeX source string for a paper, keyed by the BibTeX
    citation key (``bibtex_id``).  This table is the FK target for
    :class:`Paper`.

    :param bibtex_id: BibTeX citation key — the user-facing unique identifier.
    :param bibtex: Full BibTeX source string.  Unique to prevent duplicate
        entries for the same citation key.
    """

    __tablename__ = "bib"

    bibtex_id: Mapped[str] = mapped_column(Text, primary_key=True)
    bibtex: Mapped[str] = mapped_column(Text, unique=True)


class Paper(Base):
    """Paper metadata table.

    One row per publication record.  The :attr:`bibtex_id` column is a
    foreign key into :class:`Bib`.

    :param id: Surrogate integer primary key.
    :param title: Publication title.
    :param contents: One-sentence summary of the paper.
    :param bibtex_id: BibTeX citation key; FK → ``bib.bibtex_id``.
    """

    __tablename__ = "papers"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    title: Mapped[str | None] = mapped_column(Text)
    contents: Mapped[str | None] = mapped_column(Text)
    bibtex_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("bib.bibtex_id")
    )


class Author(Base):
    """Author name table.

    One row per distinct author name in ``"Last, First"`` form.  The same
    string is treated as the same author — deduplication is by string equality
    (documented limitation).

    :param id: Surrogate integer primary key.
    :param author: Author name in ``"Last, First"`` format.
    """

    __tablename__ = "authors_id"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    author: Mapped[str | None] = mapped_column(Text)


class AuthorPaper(Base):
    """Many-to-many link between authors and papers.

    No DDL foreign keys — preserved from legacy schema.  The application
    enforces referential integrity at the service layer.

    :param id: Surrogate integer primary key.
    :param author_id: References ``authors_id.id`` (no DDL FK).
    :param paper_id: References ``papers.id`` (no DDL FK).
    """

    __tablename__ = "authors_papers"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    author_id: Mapped[int | None] = mapped_column()
    paper_id: Mapped[int | None] = mapped_column()
