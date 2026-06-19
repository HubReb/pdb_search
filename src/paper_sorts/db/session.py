"""Engine factory and session context manager (persistence layer).

The session is context-managed so it is always closed deterministically, with a
commit on clean exit and a rollback on exception — there are no long-lived
sessions (constitution Principle IV).
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session


def make_engine(database_url: str) -> Engine:
    """Create a SQLAlchemy engine for the given database URL.

    :param database_url: a SQLAlchemy URL, e.g.
        ``postgresql+psycopg://user:pass@host/dbname``.
    :returns: a configured :class:`sqlalchemy.Engine`.
    """
    return create_engine(database_url, future=True)


@contextmanager
def with_session(engine: Engine) -> Iterator[Session]:
    """Yield a session, committing on success and rolling back on error.

    :param engine: the engine to bind the session to.
    :yields: an open :class:`sqlalchemy.orm.Session`.
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
