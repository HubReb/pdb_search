"""Engine creation and the session context manager for the persistence layer.

This module is the single place that constructs SQLAlchemy engines and sessions.
Sessions are always context-managed: committed on success, rolled back on
exception, and closed deterministically (no long-lived sessions).
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session


def create_db_engine(database_url: str) -> Engine:
    """Create a SQLAlchemy engine for the given database URL.

    :param database_url: a SQLAlchemy URL, e.g.
        ``postgresql+psycopg://user:pw@host/db``.
    :return: a configured engine.
    """
    return create_engine(database_url, future=True)


@contextmanager
def with_session(engine: Engine) -> Iterator[Session]:
    """Yield a session that commits on success and rolls back on error.

    :param engine: the engine to bind the session to.
    :return: a context manager yielding an open session.
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
