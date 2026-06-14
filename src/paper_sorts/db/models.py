"""SQLAlchemy 2.x ORM models for the paper_sorts database schema.

Four tables, preserving the original DDL exactly:
- bib(bibtex_id PK, bibtex UNIQUE)
- papers(id PK, title, contents, bibtex_id FK→bib.bibtex_id)
- authors_id(id PK, author)
- authors_papers(id PK, author_id INT, paper_id INT)  — NO DDL FKs on author_id/paper_id

Schema preservation contract (do NOT add without explicit spec change):
- No NOT NULL constraints outside primary keys
- No DDL foreign keys on authors_papers
- No indexes beyond existing primary keys
"""

from __future__ import annotations

from sqlalchemy import ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


class Bib(Base):
    """BibTeX entry table.

    Maps to the ``bib`` table. The bibtex_id is the user-facing unique key
    for a publication; the bibtex field holds the full BibTeX source string.
    """

    __tablename__ = "bib"
    __table_args__ = (UniqueConstraint("bibtex", name="bib_bibtex_unique"),)

    bibtex_id: Mapped[str] = mapped_column(String, primary_key=True)
    bibtex: Mapped[str] = mapped_column(Text)


class Paper(Base):
    """Publication record table.

    Maps to the ``papers`` table. Each paper references exactly one BibTeX
    entry via ``bibtex_id`` (DDL foreign key).
    """

    __tablename__ = "papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(Text)
    contents: Mapped[str] = mapped_column(Text)
    bibtex_id: Mapped[str] = mapped_column(
        String, ForeignKey("bib.bibtex_id", name="fk_bibtex_id")
    )


class Author(Base):
    """Author identity table.

    Maps to the ``authors_id`` table. Author names are stored in "Last, First" form.
    Two entries with identical names are treated as the same author (known limitation).
    """

    __tablename__ = "authors_id"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author: Mapped[str] = mapped_column(Text)


class AuthorPaper(Base):
    """Many-to-many link table between authors and papers.

    Maps to the ``authors_papers`` table. author_id and paper_id are intentionally
    plain Integer columns with NO DDL foreign-key constraints — this matches the
    original schema and must not be changed (schema preservation contract).
    """

    __tablename__ = "authors_papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author_id: Mapped[int] = mapped_column(Integer)  # NO DDL FK — by design
    paper_id: Mapped[int] = mapped_column(Integer)   # NO DDL FK — by design
