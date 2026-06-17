"""Engine and session management for the persistence layer.

This module owns the single point at which a SQLAlchemy ``Engine`` and
``Session`` are created. The :func:`with_session` context manager commits on
success, rolls back on any exception, and always closes the session
deterministically (constitution Principle IV — no long-lived sessions).
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker


def create_db_engine(database_url: str, *, echo: bool = False) -> Engine:
    """Create a SQLAlchemy engine for the given database URL.

    :param database_url: a ``postgresql+psycopg://…`` connection URL.
    :param echo: whether SQLAlchemy should echo emitted SQL (debugging only).
    :returns: a configured :class:`~sqlalchemy.Engine`.
    """
    return create_engine(database_url, echo=echo, future=True)


@contextmanager
def with_session(engine: Engine) -> Iterator[Session]:
    """Yield a session that commits on success and rolls back on error.

    :param engine: the engine to bind the session to.
    :yields: an open :class:`~sqlalchemy.orm.Session`.
    :raises Exception: re-raises any exception after rolling back.
    """
    factory = sessionmaker(bind=engine, future=True)
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
