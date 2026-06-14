"""SQLAlchemy 2.x ORM models for paper_sorts.

Schema preserved verbatim from legacy DatabaseConnector.create_tables():
  - bib(bibtex_id PK, bibtex UNIQUE)
  - papers(id serial PK, title, contents, bibtex_id FK→bib.bibtex_id)
  - authors_id(id serial PK, author)
  - authors_papers(id serial PK, author_id int, paper_id int)

Schema-preservation contract:
  - No NOT NULL constraints outside primary keys.
  - No DDL foreign keys on authors_papers.
  - No indexes beyond the original primary keys.
"""

from sqlalchemy import ForeignKey, Integer, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


class Bib(Base):
    """BibTeX entry keyed by bibtex_id.

    :param bibtex_id: BibTeX citation key (primary key, user-facing identifier).
    :param bibtex: Full BibTeX source string (must be unique per bib record).
    """

    __tablename__ = "bib"

    bibtex_id: Mapped[str] = mapped_column(Text, primary_key=True)
    bibtex: Mapped[str] = mapped_column(Text, unique=True)

    papers: Mapped[list["Paper"]] = relationship("Paper", back_populates="bib")


class Paper(Base):
    """Publication record.

    :param id: Internal serial primary key.
    :param title: Publication title (no NOT NULL constraint).
    :param contents: Summary / abstract text (no NOT NULL constraint).
    :param bibtex_id: FK reference into bib.bibtex_id.
    """

    __tablename__ = "papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str | None] = mapped_column(Text)
    contents: Mapped[str | None] = mapped_column(Text)
    bibtex_id: Mapped[str | None] = mapped_column(Text, ForeignKey("bib.bibtex_id"))

    bib: Mapped["Bib | None"] = relationship("Bib", back_populates="papers")


class Author(Base):
    """Author record in "Last, First" form.

    :param id: Internal serial primary key.
    :param author: Author name in "Last, First" format.
    """

    __tablename__ = "authors_id"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author: Mapped[str | None] = mapped_column(Text)


class AuthorPaper(Base):
    """Many-to-many link between Author and Paper.

    Note: No DDL foreign keys — preserving original schema contract.

    :param id: Internal serial primary key.
    :param author_id: References authors_id.id (no DDL FK constraint).
    :param paper_id: References papers.id (no DDL FK constraint).
    """

    __tablename__ = "authors_papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author_id: Mapped[int | None] = mapped_column(Integer)
    paper_id: Mapped[int | None] = mapped_column(Integer)
