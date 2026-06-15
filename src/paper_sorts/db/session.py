"""Engine factory and the ``with_session`` context manager.

This is the only place that opens a :class:`~sqlalchemy.orm.Session`. The context manager
commits on success and rolls back on any exception, and the session is always closed on exit —
satisfying the constitution's deterministic-session-close requirement. No connection pooling
beyond SQLAlchemy's default is configured.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session


def create_db_engine(database_url: str) -> Engine:
    """Create a SQLAlchemy engine for the given database URL.

    :param database_url: a SQLAlchemy URL, e.g. ``postgresql+psycopg://user:pw@host/db``.
    :return: a configured :class:`~sqlalchemy.Engine`.
    """
    return create_engine(database_url, future=True)


@contextmanager
def with_session(engine: Engine) -> Iterator[Session]:
    """Yield a session, committing on success and rolling back on exception.

    :param engine: the engine to bind the session to.
    :return: a context manager yielding an open :class:`~sqlalchemy.orm.Session`.
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
