"""SQLAlchemy 2.x declarative models for the four-table paper schema.

The schema is preserved verbatim from the legacy ``create_tables()`` DDL. The
preservation contract (see ``specs/001-modernize-stack/data-model.md``) forbids:

* ``NOT NULL`` on any column outside a primary key,
* DDL foreign keys on ``authors_papers`` (the many-to-many link table),
* indexes beyond the original primary keys and the ``bib.bibtex`` UNIQUE.

The only declared foreign key is ``papers.bibtex_id -> bib.bibtex_id``.
"""

from __future__ import annotations

from sqlalchemy import ForeignKey, Integer, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


class Bib(Base):
    """A full BibTeX source string, keyed by its BibTeX key.

    :ivar bibtex_id: the BibTeX key — the user-facing unique identifier (PK).
    :ivar bibtex: the full BibTeX source string (UNIQUE, nullable).
    """

    __tablename__ = "bib"
    __table_args__ = (UniqueConstraint("bibtex", name="bib_bibtex_key"),)

    bibtex_id: Mapped[str] = mapped_column(Text, primary_key=True)
    bibtex: Mapped[str | None] = mapped_column(Text, nullable=True)

    papers: Mapped[list[Paper]] = relationship(back_populates="bib")


class Paper(Base):
    """A publication record.

    :ivar id: internal identity (PK, SERIAL).
    :ivar title: publication title (nullable).
    :ivar contents: one-sentence summary (nullable).
    :ivar bibtex_id: FK to :class:`Bib` (nullable).
    """

    __tablename__ = "papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    contents: Mapped[str | None] = mapped_column(Text, nullable=True)
    bibtex_id: Mapped[str | None] = mapped_column(
        Text, ForeignKey("bib.bibtex_id", name="fk_bibtex_id"), nullable=True
    )

    bib: Mapped[Bib | None] = relationship(back_populates="papers")


class Author(Base):
    """A credited person.

    :ivar id: internal identity (PK, SERIAL).
    :ivar author: name in ``"Last, First"`` form (nullable).
    """

    __tablename__ = "authors_id"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author: Mapped[str | None] = mapped_column(Text, nullable=True)


class AuthorPaper(Base):
    """Many-to-many link between papers and authors.

    Intentionally carries **no** DDL foreign keys on ``author_id``/``paper_id``
    (schema-preservation contract); they reference ``authors_id.id`` and
    ``papers.id`` only logically.

    :ivar id: link-row identity (PK, SERIAL).
    :ivar author_id: logical reference to ``authors_id.id`` (nullable, no FK).
    :ivar paper_id: logical reference to ``papers.id`` (nullable, no FK).
    """

    __tablename__ = "authors_papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    paper_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
