"""SQLAlchemy 2.x ORM models for the four preserved tables.

This module is part of the persistence layer and is one of the few places
permitted to import :mod:`sqlalchemy`. The table shapes are a verbatim port of
the legacy DDL: no NOT NULL outside primary keys, no foreign keys on
``authors_papers``, and no indexes beyond the original primary keys and the
single ``bib.bibtex`` UNIQUE constraint.
"""

from __future__ import annotations

from sqlalchemy import ForeignKey, Integer, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


class Bib(Base):
    """A BibTeX entry, keyed by its user-facing BibTeX key."""

    __tablename__ = "bib"

    bibtex_id: Mapped[str] = mapped_column(Text, primary_key=True)
    bibtex: Mapped[str] = mapped_column(Text)

    __table_args__ = (UniqueConstraint("bibtex", name="bib_bibtex_key"),)


class Paper(Base):
    """A publication record, linked to exactly one :class:`Bib` entry."""

    __tablename__ = "papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str | None] = mapped_column(Text)
    contents: Mapped[str | None] = mapped_column(Text)
    bibtex_id: Mapped[str | None] = mapped_column(Text, ForeignKey("bib.bibtex_id"))


class Author(Base):
    """An author name in ``"Last, First"`` form (table ``authors_id``)."""

    __tablename__ = "authors_id"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author: Mapped[str | None] = mapped_column(Text)


class AuthorPaper(Base):
    """The many-to-many link between authors and papers.

    Note: no DDL foreign keys here — preserved from the legacy schema. The
    relationship is expressed only at the ORM level for query convenience.
    """

    __tablename__ = "authors_papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author_id: Mapped[int | None] = mapped_column(Integer)
    paper_id: Mapped[int | None] = mapped_column(Integer)
