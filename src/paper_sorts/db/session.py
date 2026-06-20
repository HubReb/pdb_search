"""Database session management for paper_sorts.

Only this module (and the rest of db/) may import sqlalchemy.
"""

from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session


def get_engine(database_url: str) -> Engine:
    """Create a SQLAlchemy engine for the given database URL.

    :param database_url: SQLAlchemy-compatible PostgreSQL URL,
        e.g. ``postgresql+psycopg://user:pass@localhost/dbname``.
    :returns: Configured :class:`sqlalchemy.engine.Engine` instance.
    """
    return create_engine(database_url, echo=False)


@contextmanager
def with_session(engine: Engine) -> Generator[Session, None, None]:
    """Yield an open SQLAlchemy session; commit on success, rollback on error.

    Usage::

        with with_session(engine) as session:
            session.add(some_model)

    :param engine: Active :class:`~sqlalchemy.engine.Engine`.
    :yields: An open :class:`~sqlalchemy.orm.Session`.
    :raises: Re-raises any exception after rolling back the transaction.
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
