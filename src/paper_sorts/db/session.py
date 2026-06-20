"""Database session management for paper_sorts.

Exposes :func:`get_engine` and :func:`with_session` — the only two entry
points into the persistence layer that service-layer code should need.

Per constitution Principle IV, connection pooling beyond SQLAlchemy's default
is **not** added here.  Sessions are closed deterministically via the context
manager; long-lived sessions are not a permitted optimisation.
"""

from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session


def get_engine(database_url: str) -> Engine:
    """Create and return a SQLAlchemy engine for *database_url*.

    :param database_url: A SQLAlchemy database URL, e.g.
        ``postgresql+psycopg://user:pass@host/dbname``.
    :returns: A configured :class:`sqlalchemy.Engine` instance.
    """
    return create_engine(database_url, future=True)


@contextmanager
def with_session(engine: Engine) -> Generator[Session, None, None]:
    """Yield a SQLAlchemy :class:`Session` that commits on success and rolls back on error.

    Usage::

        with with_session(engine) as session:
            session.add(some_object)
        # committed here

    :param engine: The :class:`sqlalchemy.Engine` to bind the session to.
    :yields: An open :class:`sqlalchemy.orm.Session`.
    :raises: Re-raises any exception after rolling back the transaction.
    """
    with Session(engine) as session:
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
