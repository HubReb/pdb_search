"""Database session management for paper-sorts.

Provides :func:`with_session` — a context manager that opens a
:class:`sqlalchemy.orm.Session`, commits on clean exit, and rolls back
(then re-raises) on any exception.  Sessions are closed deterministically.

Only modules under ``src/paper_sorts/db/`` may import SQLAlchemy.

Usage::

    from paper_sorts.db.session import with_session

    with with_session(database_url) as session:
        repo.create(session, ...)  # commit happens on exit
"""

from __future__ import annotations

import logging
from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


@contextmanager
def with_session(database_url: str) -> Generator[Session, None, None]:
    """Open a SQLAlchemy session, commit on success, roll back on error.

    The session is closed deterministically when the ``with`` block exits,
    regardless of whether the block succeeded or raised.

    No connection pooling beyond SQLAlchemy's default ``StaticPool`` /
    ``NullPool`` behaviour is used (constitution Principle IV).

    :param database_url: Full SQLAlchemy database URL, e.g.
        ``"postgresql+psycopg://user:pass@localhost/db"``.
    :yields: An open :class:`sqlalchemy.orm.Session`.
    :raises Exception: Re-raises any exception from the ``with`` block after
        rolling back the transaction.

    Example::

        with with_session("postgresql+psycopg://localhost/mydb") as session:
            paper = repo.get_by_id(session, 1)
    """
    engine = create_engine(database_url)
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
