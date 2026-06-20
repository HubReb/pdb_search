"""SQLAlchemy 2.x declarative ORM models for the paper_sorts database.

Four tables mirroring the legacy DDL from ``DatabaseConnector.create_tables()``:
``bib``, ``papers``, ``authors_id``, ``authors_papers``.

Schema-preservation contract (see CLAUDE.md):
- No NOT NULL constraints added beyond primary keys.
- No foreign keys added to ``authors_papers``.
- No indexes added beyond the original primary keys.
"""

from sqlalchemy import ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Shared declarative base for all ORM models."""


class Bib(Base):
    """Stores BibTeX entries keyed by their unique BibTeX identifier."""

    __tablename__ = "bib"

    bibtex_id: Mapped[str] = mapped_column(String, primary_key=True)
    bibtex: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = (UniqueConstraint("bibtex", name="uq_bib_bibtex"),)


class Paper(Base):
    """Stores publication metadata with a foreign key into the ``bib`` table."""

    __tablename__ = "papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(Text, nullable=True)
    contents: Mapped[str] = mapped_column(Text, nullable=True)
    bibtex_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("bib.bibtex_id", name="fk_bibtex_id"),
        nullable=True,
    )


class Author(Base):
    """Stores author names; ``author`` is in ``"Last, First"`` form."""

    __tablename__ = "authors_id"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author: Mapped[str] = mapped_column(Text, nullable=True)


class AuthorPaper(Base):
    """Many-to-many link between authors and papers.

    Intentionally has no DDL foreign key constraints — this matches the
    original DDL and is a schema-preservation contract.
    """

    __tablename__ = "authors_papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author_id: Mapped[int] = mapped_column(Integer, nullable=True)
    paper_id: Mapped[int] = mapped_column(Integer, nullable=True)
