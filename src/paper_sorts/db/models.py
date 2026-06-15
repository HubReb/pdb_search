"""SQLAlchemy 2.x ORM models mirroring the canonical four-table schema.

The schema is preserved verbatim from the legacy DDL, including its looseness:
text columns outside the primary keys are nullable, and ``authors_papers``
carries **no** foreign keys. Do not tighten this — the schema-preservation
contract (Constitution Principle IV) treats the original DDL as binding.

Only modules under ``paper_sorts.db`` may import SQLAlchemy (Constitution
Principle I).
"""

from __future__ import annotations

from sqlalchemy import ForeignKey, Integer, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


class Bib(Base):
    """A BibTeX entry, keyed by its BibTeX key.

    :ivar bibtex_id: the user-facing unique paper key (BibTeX key); primary key.
    :ivar bibtex: the full BibTeX source string; unique.
    """

    __tablename__ = "bib"

    bibtex_id: Mapped[str] = mapped_column(Text, primary_key=True)
    bibtex: Mapped[str | None] = mapped_column(Text, unique=True)


class Paper(Base):
    """A publication record.

    :ivar id: internal paper identity; primary key.
    :ivar title: paper title (nullable, per original DDL).
    :ivar contents: one-sentence summary (nullable, per original DDL).
    :ivar bibtex_id: foreign key into :class:`Bib`.
    """

    __tablename__ = "papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str | None] = mapped_column(Text)
    contents: Mapped[str | None] = mapped_column(Text)
    bibtex_id: Mapped[str | None] = mapped_column(
        Text, ForeignKey("bib.bibtex_id", name="fk_bibtex_id")
    )


class AuthorId(Base):
    """An author, identified internally.

    :ivar id: internal author identity; primary key.
    :ivar author: name in ``"Last, First"`` form (nullable, per original DDL).
    """

    __tablename__ = "authors_id"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author: Mapped[str | None] = mapped_column(Text)


class AuthorPaper(Base):
    """The many-to-many link between an author and a paper.

    No foreign keys are declared on ``author_id`` / ``paper_id`` — the link is
    enforced only in application code, exactly as in the original schema.

    :ivar id: link identity; primary key.
    :ivar author_id: references :data:`AuthorId.id` (no DDL FK, by contract).
    :ivar paper_id: references :data:`Paper.id` (no DDL FK, by contract).
    """

    __tablename__ = "authors_papers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author_id: Mapped[int | None] = mapped_column(Integer)
    paper_id: Mapped[int | None] = mapped_column(Integer)
