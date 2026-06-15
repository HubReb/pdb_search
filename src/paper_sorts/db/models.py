"""SQLAlchemy 2.x declarative ORM models for the four-table paper database.

The schema is preserved verbatim from the legacy DDL: four tables, the single
``papers.bibtex_id -> bib.bibtex_id`` foreign key, the ``bib.bibtex`` UNIQUE constraint, and
**no** foreign keys on the ``authors_papers`` link table. No NOT NULL is declared outside
primary keys, and no indexes beyond the existing primary keys are added.

This module is part of the persistence layer; per the constitution, only ``db/`` may import
``sqlalchemy`` or a database driver.
"""

from __future__ import annotations

from sqlalchemy import ForeignKey, Integer, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


class Bib(Base):
    """A full BibTeX source string keyed by its BibTeX key.

    :ivar bibtex_id: the user-facing unique BibTeX key (primary key).
    :ivar bibtex: the full BibTeX source string (unique).
    """

    __tablename__ = "bib"

    bibtex_id: Mapped[str] = mapped_column(Text, primary_key=True)
    bibtex: Mapped[str | None] = mapped_column(Text, unique=True)


class Paper(Base):
    """A publication record.

    :ivar id: internal surrogate identifier (primary key).
    :ivar title: the paper title.
    :ivar contents: the short summary of the paper.
    :ivar bibtex_id: foreign key into :class:`Bib` (the user-facing BibTeX key).
    """

    __tablename__ = "papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str | None] = mapped_column(Text)
    contents: Mapped[str | None] = mapped_column(Text)
    bibtex_id: Mapped[str | None] = mapped_column(
        Text, ForeignKey("bib.bibtex_id", name="fk_bibtex_id")
    )


class AuthorId(Base):
    """An author, identified by a surrogate id and stored as ``"Last, First"``.

    :ivar id: internal surrogate identifier (primary key).
    :ivar author: the author name in ``"Last, First"`` form.
    """

    __tablename__ = "authors_id"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author: Mapped[str | None] = mapped_column(Text)


class AuthorPaper(Base):
    """The many-to-many link between an author and a paper.

    The legacy link table declares no foreign keys; that shape is preserved here so the
    duplicate-author and orphan-author behaviours match the original tool.

    :ivar id: internal surrogate identifier (primary key).
    :ivar author_id: references ``authors_id.id`` (no DDL foreign key).
    :ivar paper_id: references ``papers.id`` (no DDL foreign key).
    """

    __tablename__ = "authors_papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author_id: Mapped[int | None] = mapped_column(Integer)
    paper_id: Mapped[int | None] = mapped_column(Integer)
