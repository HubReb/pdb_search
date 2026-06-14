"""SQLAlchemy 2.x ORM models for paper_sorts.

Four tables, schema-preserving port of ``DatabaseConnector.create_tables()``:

    papers(id, title, contents, bibtex_id -> bib.bibtex_id)
    bib(bibtex_id PK, bibtex UNIQUE)
    authors_id(id, author)
    authors_papers(id, author_id, paper_id)  -- no DDL FKs on author_id/paper_id

Schema-preservation contract:
    - No NOT NULL columns added outside primary keys.
    - No DDL FKs added to authors_papers.
    - No indexes added beyond the existing primary keys and unique constraints.
"""

from __future__ import annotations

from sqlalchemy import ForeignKey, Integer, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Shared declarative base for all ORM models."""


class Bib(Base):
    """BibTeX entry table.

    :param bibtex_id: primary key; the cite key (e.g. ``Wang2021``).
    :param bibtex: full BibTeX source string; UNIQUE constraint.
    """

    __tablename__ = "bib"

    bibtex_id: Mapped[str] = mapped_column(Text, primary_key=True)
    bibtex: Mapped[str] = mapped_column(Text, nullable=True)

    __table_args__ = (UniqueConstraint("bibtex", name="uq_bib_bibtex"),)


class Paper(Base):
    """Publication record table.

    :param id: auto-incrementing primary key.
    :param title: title of the publication (not unique by constraint).
    :param contents: one-sentence summary of the publication.
    :param bibtex_id: foreign key into ``bib.bibtex_id``.
    """

    __tablename__ = "papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    contents: Mapped[str | None] = mapped_column(Text, nullable=True)
    bibtex_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("bib.bibtex_id", name="fk_bibtex_id"),
        nullable=True,
    )


class Author(Base):
    """Author identity table.

    :param id: auto-incrementing primary key.
    :param author: author name in ``"Last, First"`` form (not unique by constraint).
    """

    __tablename__ = "authors_id"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author: Mapped[str | None] = mapped_column(Text, nullable=True)


class AuthorPaper(Base):
    """Many-to-many link between authors and papers.

    No DDL foreign keys on ``author_id`` or ``paper_id`` (schema-preservation rule).

    :param id: auto-incrementing primary key.
    :param author_id: integer ID referencing ``authors_id.id`` (no DDL FK).
    :param paper_id: integer ID referencing ``papers.id`` (no DDL FK).
    """

    __tablename__ = "authors_papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    paper_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
