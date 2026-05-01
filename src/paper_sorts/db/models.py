"""SQLAlchemy 2.x ORM models for the paper-sorts schema.

The mapping mirrors the original ``DatabaseConnector.create_tables()`` DDL exactly:
no ``NOT NULL`` outside primary keys, the only foreign key is
``papers.bibtex_id -> bib.bibtex_id``, and ``authors_papers`` carries no DDL
foreign keys. ORM navigation across the link table uses explicit
``primaryjoin``/``secondaryjoin`` with ``foreign()`` annotations so SQLAlchemy
can join without those DDL constraints. See
``specs/001-modernize-stack/data-model.md`` for the invariant table.
"""

from sqlalchemy import ForeignKey, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Declarative base shared by every ORM-mapped model in this package.

    ``type_annotation_map`` pins ``str`` to ``TEXT`` so the ORM-emitted column
    types match the verbatim DDL in ``migrations/versions/001_initial_schema.py``.
    Without this, SQLAlchemy 2.x would default ``Mapped[str]`` to ``VARCHAR``,
    which would diverge from the original ``create_tables()`` and would surface
    as spurious diffs under ``alembic revision --autogenerate``.
    """

    type_annotation_map = {  # noqa: RUF012  # SQLAlchemy reads this as a class attribute
        str: Text,
    }


class BibEntry(Base):
    """A row in the ``bib`` table — full BibTeX source keyed by a citation id."""

    __tablename__ = "bib"

    bibtex_id: Mapped[str] = mapped_column(primary_key=True)
    bibtex: Mapped[str | None] = mapped_column(unique=True)


class Paper(Base):
    """A row in the ``papers`` table — title, summary, and link to its BibTeX source."""

    __tablename__ = "papers"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str | None]
    contents: Mapped[str | None]
    bibtex_id: Mapped[str | None] = mapped_column(ForeignKey("bib.bibtex_id"))

    bib_entry: Mapped["BibEntry | None"] = relationship()
    authors: Mapped[list["Author"]] = relationship(
        secondary="authors_papers",
        primaryjoin="Paper.id == foreign(Authorship.paper_id)",
        secondaryjoin="Author.id == foreign(Authorship.author_id)",
        viewonly=False,
    )


class Author(Base):
    """A row in the ``authors_id`` table — one author name, deduplicated by exact string match."""

    __tablename__ = "authors_id"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str | None] = mapped_column("author")


class Authorship(Base):
    """A row in the ``authors_papers`` link table — many-to-many between Paper and Author."""

    __tablename__ = "authors_papers"

    id: Mapped[int] = mapped_column(primary_key=True)
    author_id: Mapped[int | None]
    paper_id: Mapped[int | None]
