"""SQLAlchemy 2.x ORM models for paper_sorts.

Declares the four database tables exactly as in the canonical legacy DDL:
  bib, papers, authors_id, authors_papers.

Schema-preservation contract: no NOT NULL beyond primary keys, no FK on
authors_papers, no new indexes.
"""

from __future__ import annotations

from sqlalchemy import ForeignKey, Integer, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Shared declarative base for all ORM models."""


class Bib(Base):
    """BibTeX entry keyed by the citation key string.

    Attributes:
        bibtex_id: Primary key; the BibTeX citation key (e.g. 'Wang2021LargeScaleSA').
        bibtex: Full BibTeX source string. UNIQUE constraint matches original DDL.
    """

    __tablename__ = "bib"

    bibtex_id: Mapped[str] = mapped_column(Text, primary_key=True)
    bibtex: Mapped[str | None] = mapped_column(Text, unique=True, nullable=True)


class Paper(Base):
    """Publication record.

    Attributes:
        id: Auto-incrementing surrogate primary key.
        title: Paper title. Nullable (preserved from original schema).
        contents: Summary / abstract. Nullable.
        bibtex_id: FK to bib.bibtex_id. Nullable (preserved).
    """

    __tablename__ = "papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    contents: Mapped[str | None] = mapped_column(Text, nullable=True)
    bibtex_id: Mapped[str | None] = mapped_column(
        Text, ForeignKey("bib.bibtex_id"), nullable=True, name="bibtex_id"
    )


class Author(Base):
    """Author record stored in 'Last, First' form.

    Attributes:
        id: Auto-incrementing surrogate primary key.
        author: Author name in 'Last, First' format. Nullable (original schema).

    Note:
        Duplicate 'Last, First' strings are treated as the same author
        (current behaviour; documented limitation).
    """

    __tablename__ = "authors_id"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author: Mapped[str | None] = mapped_column(Text, nullable=True)


class AuthorPaper(Base):
    """Many-to-many link between authors and papers.

    No DDL foreign keys — schema-preservation contract requires this.

    Attributes:
        id: Auto-incrementing surrogate primary key.
        author_id: Reference to authors_id.id (no FK constraint).
        paper_id: Reference to papers.id (no FK constraint).
    """

    __tablename__ = "authors_papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    paper_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
