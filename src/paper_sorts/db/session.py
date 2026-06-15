"""Engine creation and the session context manager.

The session lifetime is deterministically bounded: :func:`with_session` commits
on success, rolls back on any exception, and always closes the session
(Constitution Principle IV — no long-lived sessions, no leaked connections).
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session


def create_db_engine(database_url: str) -> Engine:
    """Create a SQLAlchemy engine for the given URL.

    :param database_url: a SQLAlchemy URL, e.g.
        ``postgresql+psycopg://user:pass@host:port/db``.
    :return: a configured :class:`~sqlalchemy.Engine`.
    """
    return create_engine(database_url, future=True)


@contextmanager
def with_session(engine: Engine) -> Iterator[Session]:
    """Yield a session that commits on success and rolls back on error.

    :param engine: the engine to bind the session to.
    :return: a context manager yielding an open :class:`~sqlalchemy.orm.Session`.
    :raises Exception: re-raises any exception from the body after rolling back.
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
