"""Database session management for paper_sorts.

Exposes:
  - create_engine_from_url: build a SQLAlchemy Engine from a DSN string.
  - with_session: context manager that commits on success, rolls back on error.
"""

from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session


def create_engine_from_url(database_url: str) -> Engine:
    """Create a SQLAlchemy Engine from a database URL.

    :param database_url: PostgreSQL DSN in the form
        ``postgresql+psycopg://user:pass@host:port/dbname``.
    :returns: Configured SQLAlchemy Engine (no connection pool beyond default).
    :raises sqlalchemy.exc.ArgumentError: if the URL is malformed.
    """
    return create_engine(database_url, echo=False)


@contextmanager
def with_session(engine: Engine) -> Generator[Session, None, None]:
    """Context manager yielding a SQLAlchemy Session.

    Commits the transaction on successful exit; rolls back on any exception.
    The session is closed deterministically in the finally block.

    :param engine: A SQLAlchemy Engine to bind the session to.
    :yields: An active :class:`sqlalchemy.orm.Session`.
    :raises Exception: Re-raises any exception after rolling back.
    """
    session = Session(engine)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
