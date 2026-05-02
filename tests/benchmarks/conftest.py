"""Bench-specific fixtures and pytest options for SC-006.

Shares the session-scoped ``postgresql_proc`` from ``tests/conftest.py``
and provides ``modern_db_env`` — a fresh, alembic-migrated, seeded
database plus the ``pdbsearch`` invocation parameters the in-process
helpers and subprocess runners need.

The legacy ``legacy_db_env`` fixture and its ``DatabaseConnector``
seeding path were retired with T026 (the legacy package was removed);
``baseline.json`` from T008 remains the frozen reference and is the
target ``--baseline-compare`` reads. Re-recording would require
restoring the legacy stack and is therefore not supported.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from pytest_postgresql.executor import PostgreSQLExecutor
from pytest_postgresql.janitor import DatabaseJanitor
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from paper_sorts.db.repositories import PaperCreate, PaperRepository
from paper_sorts.db.session import make_session_factory
from tests.fixtures.seed_papers import SEED_PAPERS


def pytest_addoption(parser: pytest.Parser) -> None:
    """Register --baseline-record (T008) and --baseline-compare (T046) flags."""
    parser.addoption(
        "--baseline-record",
        action="store_true",
        default=False,
        help="Record per-operation timings to tests/benchmarks/baseline.json (T008).",
    )
    parser.addoption(
        "--baseline-compare",
        action="store_true",
        default=False,
        help="Compare per-operation timings against tests/benchmarks/baseline.json (T046).",
    )


@pytest.fixture
def baseline_record(request: pytest.FixtureRequest) -> bool:
    """Return whether ``--baseline-record`` was passed on the pytest command line."""
    return bool(request.config.getoption("--baseline-record"))


@pytest.fixture
def baseline_compare(request: pytest.FixtureRequest) -> bool:
    """Return whether ``--baseline-compare`` was passed on the pytest command line."""
    return bool(request.config.getoption("--baseline-compare"))


@pytest.fixture
def modern_db_env(postgresql_proc: PostgreSQLExecutor, tmp_path: Path) -> Iterator[dict[str, Any]]:
    """Yield a fresh, migrated, seeded ephemeral PG plus modern bench parameters.

    The yielded dict contains:

    - ``db_url``: SQLAlchemy URL the ``pdbsearch --database-url`` flag accepts.
    - ``factory``: a :func:`sessionmaker` for in-process timing helpers.
    - ``tmp_path``: working directory the subprocess runs in.
    """
    db_name = "pdbsearch_bench_modern"
    with DatabaseJanitor(
        user=postgresql_proc.user,
        host=postgresql_proc.host,
        port=postgresql_proc.port,
        dbname=db_name,
        version=postgresql_proc.version,
        password=postgresql_proc.password,
    ):
        db_url = (
            f"postgresql+psycopg://{postgresql_proc.user}"
            f"@{postgresql_proc.host}:{postgresql_proc.port}/{db_name}"
        )
        engine = create_engine(db_url, future=True)
        cfg = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
        with engine.begin() as connection:
            cfg.attributes["connection"] = connection
            command.upgrade(cfg, "head")

        with Session(bind=engine) as session:
            repo = PaperRepository(session)
            for sp in SEED_PAPERS:
                repo.add(
                    PaperCreate(
                        title=sp.title,
                        contents=sp.contents,
                        bibtex_id=sp.bibtex_id,
                        bibtex=sp.bibtex,
                        authors=tuple(sp.authors),
                    )
                )
            session.commit()

        yield {
            "db_url": db_url,
            "factory": make_session_factory(engine),
            "tmp_path": tmp_path,
        }
        engine.dispose()
