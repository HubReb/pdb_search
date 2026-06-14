"""SQLAlchemy session management for paper_sorts.

Only modules under ``src/paper_sorts/db/`` may import from this module.
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import Session


@contextmanager
def with_session(database_url: str) -> Generator[Session, None, None]:
    """Context manager that yields a SQLAlchemy :class:`Session`.

    Commits the transaction on clean exit; rolls back and re-raises on any
    exception.  The session is always closed on exit (no connection leaks).

    Usage::

        with with_session(url) as session:
            session.add(some_object)
            # commit happens automatically on exit

    :param database_url: SQLAlchemy-compatible database URL
        (e.g. ``postgresql+psycopg://user:pass@host/dbname``).
    :yields: an open :class:`sqlalchemy.orm.Session` bound to *database_url*.
    :raises Exception: re-raises any exception that occurs inside the block
        after rolling back the transaction.
    """
    engine = create_engine(database_url, pool_pre_ping=True)
    session = Session(engine)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
        engine.dispose()
