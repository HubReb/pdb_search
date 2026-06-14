"""SQLAlchemy 2.x ORM models for paper_sorts.

Defines four mapped classes mirroring the existing four-table schema verbatim:
``bib``, ``papers``, ``authors_id``, ``authors_papers``.

Schema-preservation contract:
- No NOT NULL constraints added outside primary keys.
- No DDL foreign keys added to ``authors_papers``.
- No indexes added beyond the existing primary keys.
"""

from __future__ import annotations

from sqlalchemy import ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


class Bib(Base):
    """ORM model for the ``bib`` table.

    Stores BibTeX source strings, keyed by the BibTeX citation key.

    :param bibtex_id: Primary key — the BibTeX citation key (e.g. ``Wang2021``).
    :param bibtex: Full BibTeX source string.  Must be unique across rows.
    :param paper: Back-reference to the :class:`Paper` that references this entry.
    """

    __tablename__ = "bib"

    bibtex_id: Mapped[str] = mapped_column(primary_key=True)
    bibtex: Mapped[str] = mapped_column(unique=True)
    paper: Mapped[list[Paper]] = relationship(back_populates="bib_entry")


class Paper(Base):
    """ORM model for the ``papers`` table.

    One row per publication.  References one :class:`Bib` entry.

    :param id: Auto-increment primary key.
    :param title: Publication title (may be NULL — schema preservation).
    :param contents: Short summary / abstract (may be NULL).
    :param bibtex_id: Foreign key into :class:`Bib` (may be NULL).
    :param bib_entry: Relationship to the :class:`Bib` entry.
    :param author_links: Relationship to :class:`AuthorPaper` join rows.
    """

    __tablename__ = "papers"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    title: Mapped[str | None] = mapped_column(nullable=True, default=None)
    contents: Mapped[str | None] = mapped_column(nullable=True, default=None)
    bibtex_id: Mapped[str | None] = mapped_column(
        ForeignKey("bib.bibtex_id"), nullable=True, default=None
    )
    bib_entry: Mapped[Bib | None] = relationship(back_populates="paper")
    author_links: Mapped[list[AuthorPaper]] = relationship(
        back_populates="paper",
        cascade="all, delete-orphan",
        foreign_keys="[AuthorPaper.paper_id]",
        primaryjoin="Paper.id == AuthorPaper.paper_id",
    )


class Author(Base):
    """ORM model for the ``authors_id`` table.

    One row per unique author name.

    :param id: Auto-increment primary key.
    :param author: Author name in ``"Last, First"`` format (may be NULL).
    :param paper_links: Relationship to :class:`AuthorPaper` join rows.
    """

    __tablename__ = "authors_id"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    author: Mapped[str | None] = mapped_column(nullable=True, default=None)
    paper_links: Mapped[list[AuthorPaper]] = relationship(
        back_populates="author_obj",
        foreign_keys="[AuthorPaper.author_id]",
        primaryjoin="Author.id == AuthorPaper.author_id",
    )


class AuthorPaper(Base):
    """ORM model for the ``authors_papers`` many-to-many join table.

    No DDL foreign keys — preserved from the original schema.

    :param id: Auto-increment primary key.
    :param author_id: Integer ID from ``authors_id`` (no DDL FK by design).
    :param paper_id: Integer ID from ``papers`` (no DDL FK by design).
    :param author_obj: Relationship to :class:`Author`.
    :param paper: Relationship to :class:`Paper`.
    """

    __tablename__ = "authors_papers"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    author_id: Mapped[int | None] = mapped_column(nullable=True, default=None)
    paper_id: Mapped[int | None] = mapped_column(nullable=True, default=None)
    # Relationships use explicit foreign_keys to avoid SA ambiguity since
    # there are no DDL FK constraints on this table.
    author_obj: Mapped[Author | None] = relationship(
        back_populates="paper_links",
        foreign_keys="[AuthorPaper.author_id]",
        primaryjoin="AuthorPaper.author_id == Author.id",
    )
    paper: Mapped[Paper | None] = relationship(
        back_populates="author_links",
        foreign_keys="[AuthorPaper.paper_id]",
        primaryjoin="AuthorPaper.paper_id == Paper.id",
    )
