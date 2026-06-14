"""SQLAlchemy engine and session management for paper_sorts.

Provides ``get_engine`` to create an engine from a DSN, and ``with_session``
as a context manager that commits on success and rolls back on exception.
Sessions are deterministically closed — no long-lived sessions permitted.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session


def get_engine(database_url: str) -> Engine:
    """Create a SQLAlchemy engine for the given database URL.

    Connection pooling uses SQLAlchemy defaults (NullPool is NOT used — the
    constitution prohibits adding custom pooling beyond the default).

    :param database_url: PostgreSQL DSN (postgresql+psycopg://...)
    :type database_url: str
    :return: configured SQLAlchemy engine
    :rtype: Engine
    """
    return create_engine(database_url, echo=False)


@contextmanager
def with_session(engine: Engine) -> Generator[Session, None, None]:
    """Provide a transactional database session as a context manager.

    Commits on clean exit; rolls back and re-raises on any exception.
    The session is always closed when the context exits.

    :param engine: SQLAlchemy engine to bind the session to
    :type engine: Engine
    :raises Exception: re-raises any exception from the context body after rollback
    :yields: an active SQLAlchemy Session
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
