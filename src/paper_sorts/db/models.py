"""SQLAlchemy 2.x ORM models for the paper_sorts database.

Four tables matching the existing schema exactly (schema preservation contract):
- papers: publication records
- bib: BibTeX entries keyed by bibtex_id
- authors_id: author names
- authors_papers: many-to-many link (no DDL FKs per schema preservation rule)

Invariants:
- No NOT NULL constraints outside primary keys.
- No foreign keys on authors_papers.
- No indexes beyond the original primary keys.
"""

from sqlalchemy import ForeignKey, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""

    pass


class Bib(Base):
    """BibTeX entry keyed by the bibtex identifier.

    Attributes:
        bibtex_id: Primary key; the BibTeX citation key (e.g. "Wang2021LargeScaleSA").
        bibtex: Full BibTeX source string; must be unique across all entries.
    """

    __tablename__ = "bib"

    bibtex_id: Mapped[str] = mapped_column(String, primary_key=True)
    bibtex: Mapped[str] = mapped_column(Text, unique=True, nullable=False)

    paper: Mapped["Paper"] = relationship("Paper", back_populates="bib_entry", uselist=False)


class Paper(Base):
    """Publication record linking title, summary, and BibTeX key.

    Attributes:
        id: Auto-increment serial primary key.
        title: Publication title (nullable per schema preservation).
        contents: One-sentence summary of the paper (nullable per schema preservation).
        bibtex_id: Foreign key into bib.bibtex_id (nullable per schema preservation).
    """

    __tablename__ = "papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    contents: Mapped[str | None] = mapped_column(Text, nullable=True)
    bibtex_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("bib.bibtex_id"), nullable=True
    )

    bib_entry: Mapped["Bib | None"] = relationship("Bib", back_populates="paper")


class Author(Base):
    """Author record with name in 'Last, First' format.

    Attributes:
        id: Auto-increment serial primary key.
        author: Author name string in 'Last, First' format (nullable per schema preservation).
    """

    __tablename__ = "authors_id"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author: Mapped[str | None] = mapped_column(Text, nullable=True)


class AuthorPaper(Base):
    """Many-to-many link between authors and papers.

    No DDL foreign keys per schema preservation rule — application logic handles referential
    integrity. This preserves exact parity with the original schema created by
    DatabaseConnector.create_tables().

    Attributes:
        id: Auto-increment serial primary key.
        author_id: References authors_id.id (no DDL FK).
        paper_id: References papers.id (no DDL FK).
    """

    __tablename__ = "authors_papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    paper_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
