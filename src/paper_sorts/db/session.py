"""SQLAlchemy session management for paper_sorts.

Provides a context manager factory that opens a session, commits on clean
exit, and rolls back on exception. Long-lived sessions are not permitted
(constitution Principle IV).
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker


def _make_session_factory(database_url: str) -> sessionmaker[Session]:
    """Create a sessionmaker bound to the given URL.

    Args:
        database_url: SQLAlchemy-compatible connection string.

    Returns:
        A sessionmaker that produces Session objects.
    """
    engine = create_engine(database_url)
    return sessionmaker(bind=engine)


@contextmanager
def with_session(database_url: str) -> Generator[Session, None, None]:
    """Context manager that yields a SQLAlchemy Session.

    Commits the transaction on clean exit; rolls back on any exception.
    The session (and underlying connection) is closed on exit regardless.

    Args:
        database_url: SQLAlchemy-compatible connection string.

    Yields:
        An open SQLAlchemy Session.

    Raises:
        Any exception raised inside the block is re-raised after rollback.
    """
    factory = _make_session_factory(database_url)
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
