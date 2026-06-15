"""Database session management for paper_sorts.

Provides a context-managed session that commits on success and rolls back on
exception, closing deterministically in all cases (constitution Principle IV).

Only this module and db/repositories.py may import sqlalchemy or any database
driver (constitution Principle I).
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import Session


def get_engine(url: str):  # type: ignore[no-untyped-def]
    """Create a SQLAlchemy engine for the given database URL.

    :param url: SQLAlchemy-compatible database URL.
    :return: SQLAlchemy Engine instance.
    """
    return create_engine(url, pool_pre_ping=True)


@contextmanager
def with_session(url: str) -> Generator[Session, None, None]:
    """Provide a transactional database session.

    Commits on successful exit, rolls back on exception, and always closes
    the session deterministically. No long-lived sessions are permitted.

    :param url: SQLAlchemy-compatible database URL.
    :yields: An active SQLAlchemy Session.
    :raises Exception: Re-raises any exception after rolling back.
    """
    engine = get_engine(url)
    session = Session(engine)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
        engine.dispose()
