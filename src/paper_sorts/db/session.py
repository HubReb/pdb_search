"""Database session management.

Provides :func:`with_session`, the single context-managed entry point for
opening a SQLAlchemy session. Per constitution Principle IV, sessions are
closed deterministically and never held long-lived: commit on clean exit,
rollback on exception, always close.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session


def make_engine(database_url: str) -> Engine:
    """Create a SQLAlchemy engine for the given database URL.

    :param database_url: a SQLAlchemy URL, e.g.
        ``postgresql+psycopg://user:pw@host:port/dbname``.
    :returns: a configured :class:`~sqlalchemy.Engine`.
    """
    return create_engine(database_url, future=True)


@contextmanager
def with_session(engine: Engine) -> Iterator[Session]:
    """Yield a session, committing on success and rolling back on error.

    The session is always closed when the context exits (Principle IV — no
    long-lived sessions, deterministic close).

    :param engine: the engine to bind the session to.
    :yields: an open :class:`~sqlalchemy.orm.Session`.
    :raises Exception: re-raises any exception after rolling back.
    """
    session = Session(engine, future=True)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
