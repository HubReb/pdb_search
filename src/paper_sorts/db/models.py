"""SQLAlchemy 2.x declarative models for the four-table paper schema.

The schema is preserved verbatim from the legacy DDL: no NOT NULL outside
primary keys, no DDL foreign keys on ``authors_papers``, and no indexes beyond
the original primary keys. The many-to-many relationship between papers and
authors is modelled in Python through the ``authors_papers`` link table, which
itself carries no database-level foreign keys.
"""

from __future__ import annotations

from sqlalchemy import ForeignKey, Integer, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


class Bib(Base):
    """A BibTeX entry, keyed by its BibTeX key.

    :ivar bibtex_id: the BibTeX key (primary key, user-facing identifier).
    :ivar bibtex: the full BibTeX source string (unique).
    """

    __tablename__ = "bib"

    bibtex_id: Mapped[str] = mapped_column(Text, primary_key=True)
    bibtex: Mapped[str | None] = mapped_column(Text, unique=True)

    papers: Mapped[list[Paper]] = relationship(back_populates="bib_entry")


class Paper(Base):
    """A publication record.

    :ivar id: internal identity (serial primary key).
    :ivar title: publication title.
    :ivar contents: one-sentence summary.
    :ivar bibtex_id: BibTeX key, foreign key into :class:`Bib`.
    """

    __tablename__ = "papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str | None] = mapped_column(Text)
    contents: Mapped[str | None] = mapped_column(Text)
    bibtex_id: Mapped[str | None] = mapped_column(
        Text, ForeignKey("bib.bibtex_id", name="fk_bibtex_id")
    )

    bib_entry: Mapped[Bib | None] = relationship(back_populates="papers")


class Author(Base):
    """A person credited on one or more papers.

    :ivar id: serial primary key.
    :ivar author: name in ``"Last, First"`` form.
    """

    __tablename__ = "authors_id"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author: Mapped[str | None] = mapped_column(Text)


class AuthorPaper(Base):
    """The many-to-many link between an author and a paper.

    No database-level foreign keys are declared (schema-preservation contract);
    ``author_id`` and ``paper_id`` reference :class:`Author` and :class:`Paper`
    only logically.

    :ivar id: serial primary key.
    :ivar author_id: logical reference to ``authors_id.id``.
    :ivar paper_id: logical reference to ``papers.id``.
    """

    __tablename__ = "authors_papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author_id: Mapped[int | None] = mapped_column(Integer)
    paper_id: Mapped[int | None] = mapped_column(Integer)
