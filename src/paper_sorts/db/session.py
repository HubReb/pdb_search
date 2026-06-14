"""SQLAlchemy session management for paper_sorts.

Provides a context-managed session that commits on success and rolls back
on exception. All database access in this package goes through these helpers.

Only this module (and the rest of src/paper_sorts/db/) imports sqlalchemy.
"""

from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session


def get_engine(database_url: str) -> Engine:
    """Create a SQLAlchemy engine for the given database URL.

    Args:
        database_url: SQLAlchemy-compatible connection URL,
            e.g. "postgresql+psycopg://user:pass@localhost/dbname".

    Returns:
        A new Engine instance with NullPool (no connection pooling per constitution).
    """
    from sqlalchemy.pool import NullPool

    return create_engine(database_url, poolclass=NullPool)


@contextmanager
def with_session(database_url: str) -> Generator[Session, None, None]:
    """Yield a SQLAlchemy Session, committing on success and rolling back on error.

    All database write operations must use this context manager to ensure
    consistent transaction semantics. Connections are closed deterministically
    on exit (constitution Principle IV).

    Args:
        database_url: SQLAlchemy-compatible connection URL.

    Yields:
        An active Session bound to the given database.

    Raises:
        Any exception raised inside the block will trigger a rollback before
        re-raising.
    """
    engine = get_engine(database_url)
    with Session(engine) as session:
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            engine.dispose()
