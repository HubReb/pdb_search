"""Engine, session-factory, and unit-of-work context manager for the persistence layer.

The service layer obtains a session via :func:`with_session`, which commits on
clean exit and rolls back + re-raises on any exception — the SQLAlchemy
equivalent of the legacy ``DatabaseConnector.rollback_database_addition``
discipline, but driven by ``Session.begin()`` semantics rather than bespoke
SQL.

Per constitution Principle IV (v1.3.0), no connection-pool sizing beyond the
SQLAlchemy default is configured here. Tuning the pool requires a measured
regression and a Complexity Tracking entry.
"""

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker


def make_engine(database_url: str) -> Engine:
    """Build a SQLAlchemy engine bound to ``database_url``.

    Args:
        database_url: Any SQLAlchemy-compatible URL, e.g.
            ``postgresql+psycopg://user:pw@host/db``.

    Returns:
        A configured :class:`Engine`. ``future=True`` is set explicitly even
        though it is the SQLAlchemy 2.x default, to make the 2.0-style
        intent visible at the call site.
    """
    return create_engine(database_url, future=True)


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    """Build a :class:`sessionmaker` bound to ``engine``.

    The factory is the long-lived object held by the CLI app; individual
    sessions are short-lived and acquired via :func:`with_session`.
    """
    return sessionmaker(bind=engine, future=True)


@contextmanager
def with_session(factory: sessionmaker[Session]) -> Iterator[Session]:
    """Yield a :class:`Session` that commits on success, rolls back on error.

    The single transaction wraps the whole ``with`` body. Any exception
    inside the block triggers ``rollback()`` and is re-raised; a clean exit
    triggers ``commit()``. The session is closed in either case.

    Yields:
        A fresh :class:`Session` from ``factory``.
    """
    session = factory()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    else:
        session.commit()
    finally:
        session.close()
