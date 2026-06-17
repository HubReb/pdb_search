"""SQLAlchemy 2.x ORM models for the four paper-database tables.

The schema is preserved verbatim from the original DDL: ``papers``, ``bib``,
``authors_id`` and ``authors_papers``. No NOT NULL constraints outside primary
keys, no foreign keys on ``authors_papers``, and no indexes beyond the original
primary keys and the ``bib.bibtex`` UNIQUE are added. Only modules under
``paper_sorts.db`` may import this module's SQLAlchemy types.
"""

from __future__ import annotations

from sqlalchemy import ForeignKey, Integer, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base carrying the shared metadata for all ORM models."""


class Bib(Base):
    """A full BibTeX source string, keyed by its BibTeX key.

    :ivar bibtex_id: the user-facing unique BibTeX key (primary key).
    :ivar bibtex: the verbatim BibTeX source string (unique).
    """

    __tablename__ = "bib"

    bibtex_id: Mapped[str] = mapped_column(Text, primary_key=True)
    bibtex: Mapped[str | None] = mapped_column(Text, unique=True)


class Paper(Base):
    """A publication record.

    :ivar id: internal surrogate primary key.
    :ivar title: the paper title (nullable, as in the original schema).
    :ivar contents: a one-line summary of the paper (nullable).
    :ivar bibtex_id: foreign key into :class:`Bib` (nullable).
    """

    __tablename__ = "papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str | None] = mapped_column(Text)
    contents: Mapped[str | None] = mapped_column(Text)
    bibtex_id: Mapped[str | None] = mapped_column(
        Text, ForeignKey("bib.bibtex_id", name="fk_bibtex_id")
    )


class AuthorId(Base):
    """An author, identified by a surrogate id and a ``"Last, First"`` name.

    :ivar id: internal surrogate primary key.
    :ivar author: the author's name in ``"Last, First"`` form (nullable).
    """

    __tablename__ = "authors_id"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author: Mapped[str | None] = mapped_column(Text)


class AuthorPaper(Base):
    """The many-to-many link between an author and a paper.

    The original schema deliberately carries no foreign keys on this table; that
    is preserved here.

    :ivar id: internal surrogate primary key.
    :ivar author_id: references :class:`AuthorId` by value (no DDL FK).
    :ivar paper_id: references :class:`Paper` by value (no DDL FK).
    """

    __tablename__ = "authors_papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author_id: Mapped[int | None] = mapped_column(Integer)
    paper_id: Mapped[int | None] = mapped_column(Integer)
