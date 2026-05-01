"""Bench-specific fixtures and pytest options for the SC-006 baseline.

Shares the session-scoped ``postgresql_proc`` from ``tests/conftest.py``
and adds a ``legacy_db_env`` fixture that spins up a fresh database,
seeds it through the legacy stack (``DatabaseConnector.add_entry_to_db``
for each ``SeedPaper``) and writes a Fernet-encrypted INI + key file
matching the layout that ``paper_sorts.run`` expects.
"""

from __future__ import annotations

import configparser
import io
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from cryptography.fernet import Fernet
from pytest_postgresql.executor import PostgreSQLExecutor
from pytest_postgresql.janitor import DatabaseJanitor


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
    return bool(request.config.getoption("--baseline-record"))


@pytest.fixture
def baseline_compare(request: pytest.FixtureRequest) -> bool:
    return bool(request.config.getoption("--baseline-compare"))


@pytest.fixture
def legacy_db_env(
    postgresql_proc: PostgreSQLExecutor, tmp_path: Path
) -> Iterator[dict[str, Any]]:
    """Yield a seeded ephemeral PG plus the file paths the legacy CLI needs.

    The yielded dict contains:

    - ``config_path``: Fernet-encrypted INI for ``-c``
    - ``key_path``: Fernet key for ``-k``
    - ``db_config``: plain credentials usable by ``DatabaseConnector(...)``
      for the direct-call delete benchmark
    - ``tmp_path``: the working directory the subprocess should ``cwd`` into
      so legacy log files (``db_connector_test.log``, ``interaction.log``) do
      not pollute the repository
    """
    db_name = "pdbsearch_bench"
    with DatabaseJanitor(
        user=postgresql_proc.user,
        host=postgresql_proc.host,
        port=postgresql_proc.port,
        dbname=db_name,
        version=postgresql_proc.version,
        password=postgresql_proc.password,
    ):
        db_config: dict[str, str] = {
            "dbname": db_name,
            "user": postgresql_proc.user,
            "host": postgresql_proc.host,
            "port": str(postgresql_proc.port),
            "password": postgresql_proc.password or "",
        }

        # The legacy DatabaseConnector.create_tables() emits broken DDL
        # (`bibtex text unique (bibtex)`) that PG 18 rejects, so create
        # the schema with raw SQL matching migrations/001_initial_schema.py.
        # The seeding step below still goes through the legacy stack —
        # only the DDL is bypassed.
        import psycopg2

        with psycopg2.connect(**db_config) as conn, conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS bib ("
                "bibtex_id text PRIMARY KEY, bibtex text, UNIQUE (bibtex));"
            )
            cur.execute(
                "CREATE TABLE IF NOT EXISTS papers ("
                "id SERIAL PRIMARY KEY, title text, contents text, "
                "bibtex_id text, "
                "CONSTRAINT fk_bibtex_id FOREIGN KEY (bibtex_id) "
                "REFERENCES bib(bibtex_id));"
            )
            cur.execute(
                "CREATE TABLE IF NOT EXISTS authors_id ("
                "id SERIAL PRIMARY KEY, author text);"
            )
            cur.execute(
                "CREATE TABLE IF NOT EXISTS authors_papers ("
                "id SERIAL PRIMARY KEY, author_id int, paper_id int);"
            )

        # Local import: legacy code is only present in the flat-layout tree
        # and depends on psycopg2-binary (the legacy-baseline extra).
        from paper_sorts.database_connector import DatabaseConnector

        connector = DatabaseConnector(
            db_config,
            logging.WARNING,
            logger_name="bench_seed",
            log_file=str(tmp_path / "bench_seed.log"),
        )

        from tests.fixtures.seed_papers import SEED_PAPERS

        for sp in SEED_PAPERS:
            connector.add_entry_to_db(
                sp.bibtex,
                list(sp.authors),
                sp.bibtex_id,
                sp.title,
                sp.contents,
            )

        ini = configparser.ConfigParser()
        ini["postgresql"] = db_config
        sink = io.StringIO()
        ini.write(sink)
        plaintext = sink.getvalue().encode()

        key = Fernet.generate_key()
        encrypted = Fernet(key).encrypt(plaintext)

        config_path = tmp_path / "bench_db.crypt"
        config_path.write_bytes(encrypted)
        key_path = tmp_path / "bench_key"
        key_path.write_bytes(key)

        yield {
            "config_path": config_path,
            "key_path": key_path,
            "db_config": db_config,
            "tmp_path": tmp_path,
        }
