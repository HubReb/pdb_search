"""SQLAlchemy ORM models for the paper_sorts four-table schema.

Schema preservation contract (do NOT violate):
- No new NOT NULL columns outside primary keys
- No DDL foreign keys on authors_papers
- No new indexes beyond the original primary keys

Note: AuthorPaper intentionally has no DDL foreign keys. SQLAlchemy relationships
on AuthorPaper use explicit foreign_keys and primaryjoin to work around this.
"""

from __future__ import annotations

from sqlalchemy import ForeignKey, Integer, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""

    pass


class Bib(Base):
    """BibTeX entry table.

    Stores the full BibTeX source string keyed by the BibTeX identifier.
    The bibtex_id is the canonical user-facing unique identifier for a paper.
    """

    __tablename__ = "bib"

    bibtex_id: Mapped[str] = mapped_column(Text, primary_key=True)
    bibtex: Mapped[str] = mapped_column(Text, unique=True)

    def __repr__(self) -> str:
        """Return string representation of Bib."""
        return f"Bib(bibtex_id={self.bibtex_id!r})"


class Paper(Base):
    """Paper (publication) table.

    Stores title, summary (contents), and a FK to the BibTeX entry.
    Identified internally by integer id; user-facing identifier is bibtex_id.
    """

    __tablename__ = "papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(Text)
    contents: Mapped[str] = mapped_column(Text)
    bibtex_id: Mapped[str] = mapped_column(Text, ForeignKey("bib.bibtex_id"))

    def __repr__(self) -> str:
        """Return string representation of Paper."""
        return f"Paper(id={self.id!r}, title={self.title!r})"


class Author(Base):
    """Author table.

    Stores author names in 'Last, First' form.
    Two authors with identical names are treated as the same author (known limitation).
    """

    __tablename__ = "authors_id"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author: Mapped[str] = mapped_column(Text)

    def __repr__(self) -> str:
        """Return string representation of Author."""
        return f"Author(id={self.id!r}, author={self.author!r})"


class AuthorPaper(Base):
    """Many-to-many link table between Author and Paper.

    Intentionally has no DDL foreign keys — this matches the original schema
    contract and must not be changed.
    """

    __tablename__ = "authors_papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author_id: Mapped[int] = mapped_column(Integer)
    paper_id: Mapped[int] = mapped_column(Integer)

    def __repr__(self) -> str:
        """Return string representation of AuthorPaper."""
        return f"AuthorPaper(author_id={self.author_id!r}, paper_id={self.paper_id!r})"
