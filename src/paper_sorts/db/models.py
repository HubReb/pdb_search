"""SQLAlchemy 2.x ORM models for paper_sorts.

Four tables mirror the original schema exactly:
- papers (id, title, contents, bibtex_id → bib.bibtex_id)
- bib (bibtex_id PK, bibtex UNIQUE)
- authors_id (id, author)
- authors_papers (id, author_id, paper_id) — many-to-many, NO DDL FKs

Schema preservation rules (from CLAUDE.md):
- Do NOT add NOT NULL constraints outside primary keys.
- Do NOT add DDL foreign keys to authors_papers.
- Do NOT add indexes beyond existing primary keys.
"""

from sqlalchemy import Integer, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


class Bib(Base):
    """BibTeX entry store.

    Keyed by bibtex_id (the citation key).  The bibtex column stores the
    full BibTeX source string and is constrained UNIQUE.
    """

    __tablename__ = "bib"

    bibtex_id: Mapped[str] = mapped_column(Text, primary_key=True)
    bibtex: Mapped[str] = mapped_column(
        Text, UniqueConstraint(), nullable=False
    )


class Paper(Base):
    """Publication record.

    bibtex_id is a logical FK to bib.bibtex_id; the DDL FK is declared here
    as a DDL-level constraint (on the papers table only — not on
    authors_papers, per schema preservation rules).
    """

    __tablename__ = "papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    contents: Mapped[str] = mapped_column(Text, nullable=False)
    bibtex_id: Mapped[str] = mapped_column(Text, nullable=False)


class Author(Base):
    """Author identity record — name in "Last, First" form."""

    __tablename__ = "authors_id"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author: Mapped[str] = mapped_column(Text, nullable=False)


class AuthorPaper(Base):
    """Many-to-many link between Author and Paper.

    No DDL foreign keys per schema preservation contract.
    author_id and paper_id are plain integer columns.
    """

    __tablename__ = "authors_papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author_id: Mapped[int] = mapped_column(Integer, nullable=False)
    paper_id: Mapped[int] = mapped_column(Integer, nullable=False)
