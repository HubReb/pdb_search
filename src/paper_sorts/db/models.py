"""SQLAlchemy 2.x ORM models for paper_sorts.

Maps the four existing PostgreSQL tables: papers, bib, authors_id, authors_papers.

Schema-preservation rules (hard contract from constitution):
- No NOT NULL constraints outside primary keys.
- No DDL foreign keys on authors_papers.
- No indexes beyond original primary keys.
- Column names are exactly as in the original schema (bibtex_id, not bibtext_id).
"""

from __future__ import annotations

from sqlalchemy import Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


class Paper(Base):
    """ORM model for the papers table.

    :param id: auto-increment primary key
    :param title: publication title
    :param contents: summary / abstract
    :param bibtex_id: BibTeX citation key; soft reference to bib.bibtex_id
    """

    __tablename__ = "papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str | None] = mapped_column(String, nullable=True)
    contents: Mapped[str | None] = mapped_column(Text, nullable=True)
    bibtex_id: Mapped[str | None] = mapped_column(String, nullable=True)


class BibEntry(Base):
    """ORM model for the bib table.

    :param bibtex_id: BibTeX citation key; primary key and user-facing identifier
    :param bibtex: full BibTeX source string; must be unique
    """

    __tablename__ = "bib"

    bibtex_id: Mapped[str] = mapped_column(String, primary_key=True)
    bibtex: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (UniqueConstraint("bibtex", name="uq_bib_bibtex"),)


class Author(Base):
    """ORM model for the authors_id table.

    :param id: auto-increment primary key
    :param author: author name in 'Last, First' form
    """

    __tablename__ = "authors_id"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author: Mapped[str | None] = mapped_column(String, nullable=True)


class AuthorPaper(Base):
    """ORM model for the authors_papers link table.

    No DDL foreign keys are declared — this is intentional to match the
    original schema. author_id and paper_id are soft references.

    :param id: auto-increment primary key
    :param author_id: soft reference to authors_id.id
    :param paper_id: soft reference to papers.id
    """

    __tablename__ = "authors_papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    paper_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
