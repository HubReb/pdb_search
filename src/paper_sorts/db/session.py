"""Database session management for paper_sorts.

This is the only module under ``src/paper_sorts/`` permitted to create a
SQLAlchemy :class:`~sqlalchemy.engine.Engine`.  All other modules receive an
already-constructed engine (passed from :mod:`paper_sorts.cli.app`) or a
:class:`~sqlalchemy.orm.Session` (passed from service functions).

Public API:
    :func:`get_engine` — create an Engine from a DSN string.
    :func:`with_session` — context manager that yields a Session,
        commits on success, rolls back on exception, and always closes.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session


def get_engine(database_url: str) -> Engine:
    """Create a SQLAlchemy Engine from a PostgreSQL DSN.

    The engine uses the default connection pool (5 connections, overflow 10).
    Connection pooling beyond the default is explicitly out of scope per
    the project constitution.

    :param database_url: PostgreSQL DSN, e.g.
        ``postgresql+psycopg://user:pass@host/dbname``.
    :return: A configured :class:`~sqlalchemy.engine.Engine`.
    """
    return create_engine(database_url)


@contextmanager
def with_session(engine: Engine) -> Iterator[Session]:
    """Context manager that yields a SQLAlchemy Session.

    Commits the session on normal exit; rolls back on any exception.
    The session is always closed when the block exits.

    :param engine: A :class:`~sqlalchemy.engine.Engine` to bind the session to.
    :yields: An open :class:`~sqlalchemy.orm.Session`.
    :raises: Re-raises any exception after rolling back the session.

    Example::

        with with_session(engine) as session:
            session.add(some_obj)
        # committed here
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
