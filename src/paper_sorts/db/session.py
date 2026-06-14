"""SQLAlchemy engine and session management for paper_sorts.

Only modules under src/paper_sorts/db/ may import sqlalchemy.
The engine is created from the Settings.database_url; sessions are
managed via the with_session() context manager which commits on success
and rolls back on exception, always closing deterministically.
"""

from __future__ import annotations

import logging
from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

logger = logging.getLogger(__name__)

_engine: Engine | None = None
_SessionFactory: sessionmaker[Session] | None = None


def get_engine(database_url: str) -> Engine:
    """Return a SQLAlchemy engine for the given database URL.

    Creates the engine on first call; subsequent calls with the same URL
    return the cached engine. Call reset_engine() to force recreation.

    :param database_url: PostgreSQL DSN, e.g. postgresql+psycopg://user:pw@host/db
    :return: SQLAlchemy Engine instance
    """
    global _engine, _SessionFactory  # noqa: PLW0603
    if _engine is None:
        _engine = create_engine(database_url, pool_pre_ping=True)
        _SessionFactory = sessionmaker(bind=_engine, autocommit=False, autoflush=False)
        logger.debug("Created SQLAlchemy engine for %s", database_url.split("@")[-1])
    return _engine


def reset_engine() -> None:
    """Dispose of the cached engine and session factory.

    Used in tests to ensure a fresh engine per test session.
    """
    global _engine, _SessionFactory  # noqa: PLW0603
    if _engine is not None:
        _engine.dispose()
    _engine = None
    _SessionFactory = None


@contextmanager
def with_session(database_url: str) -> Generator[Session, None, None]:
    """Context manager that provides a SQLAlchemy Session.

    Commits the transaction on clean exit; rolls back on any exception.
    Always closes the session when the block exits.

    :param database_url: PostgreSQL DSN
    :yields: an open SQLAlchemy Session
    :raises Exception: re-raises any exception after rolling back
    """
    get_engine(database_url)
    assert _SessionFactory is not None
    session = _SessionFactory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def check_connection(database_url: str) -> bool:
    """Verify the database is reachable.

    :param database_url: PostgreSQL DSN
    :return: True if a simple query succeeds; False otherwise
    """
    try:
        engine = get_engine(database_url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as exc:
        logger.error("Database connection check failed: %s", exc)
        return False
