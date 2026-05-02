"""Integration tests for the bulk-import flow (T045).

Covers four scenarios from spec User Story 5 / contract § "Subcommand: import":

1. Happy path — every cite in ``sample.tex`` resolves in ``sample.bib`` and
   lands as a row in ``papers``/``bib``/``authors_id`` plus the join rows.
2. Missing-bib-key — a cite with no matching bib entry is skipped, a
   WARNING is logged, the rest of the import continues.
3. Idempotent re-run — running a second time on the same input inserts
   zero new rows; the duplicates are caught at the service layer
   (:class:`DuplicateBibtexIdError`) and reported as ``skipped``.
4. Partial-failure — when the third yielded record fails at the service
   layer, the first two records are persisted (per-paper transaction)
   and the loop stops at the failing entry.

Each test gets its own fresh, empty PostgreSQL database via a
function-scoped :class:`DatabaseJanitor`, so the seeded keys in
``conftest.py``'s ``seeded_engine`` do not collide with the fixture
bibtex_ids reused by ``sample.tex`` / ``sample.bib``.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace

import pytest
from alembic import command
from alembic.config import Config
from pytest_postgresql.executor import PostgreSQLExecutor
from pytest_postgresql.janitor import DatabaseJanitor
from sqlalchemy import create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from paper_sorts.cli import importer as importer_cli
from paper_sorts.db.models import Author, BibEntry, Paper
from paper_sorts.db.session import make_session_factory
from paper_sorts.services import paper_service


@pytest.fixture
def fresh_engine(
    postgresql_proc: PostgreSQLExecutor, request: pytest.FixtureRequest
) -> Iterator[Engine]:
    """Yield an :class:`Engine` bound to a fresh, migrated, empty database."""
    test_id = re.sub(r"[^a-zA-Z0-9_]", "_", request.node.name).lower()[:40]
    db_name = f"imp_{test_id}"
    with DatabaseJanitor(
        user=postgresql_proc.user,
        host=postgresql_proc.host,
        port=postgresql_proc.port,
        dbname=db_name,
        version=postgresql_proc.version,
        password=postgresql_proc.password,
    ):
        url = (
            f"postgresql+psycopg://{postgresql_proc.user}"
            f"@{postgresql_proc.host}:{postgresql_proc.port}/{db_name}"
        )
        engine = create_engine(url, future=True)
        cfg = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
        with engine.begin() as connection:
            cfg.attributes["connection"] = connection
            command.upgrade(cfg, "head")
        yield engine
        engine.dispose()


def _invoke(engine: Engine, tex: Path, bib: Path) -> None:
    """Drive ``pdbsearch import`` against ``engine`` with the given files."""
    ctx = SimpleNamespace(obj=make_session_factory(engine))
    importer_cli.import_(ctx, tex, bib)  # type: ignore[arg-type]


def test_happy_path_inserts_every_resolved_cite(
    fresh_engine: Engine, capsys: pytest.CaptureFixture[str]
) -> None:
    _invoke(
        fresh_engine,
        Path(__file__).resolve().parents[1] / "fixtures" / "sample.tex",
        Path(__file__).resolve().parents[1] / "fixtures" / "sample.bib",
    )

    with Session(bind=fresh_engine) as session:
        bib_keys = set(session.execute(select(BibEntry.bibtex_id)).scalars())
        paper_keys = set(session.execute(select(Paper.bibtex_id)).scalars())
    assert bib_keys == {
        "Lee2022DirectSpeechToSpeech",
        "Wang2021LargeScaleSA",
        "Schoettler2023FairnessMT",
    }
    assert paper_keys == bib_keys

    out = capsys.readouterr().out
    assert "inserted=3" in out
    assert "skipped=0" in out
    assert "warned=0" in out


def test_missing_bib_key_skipped_with_warning(
    fresh_engine: Engine,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    tex = tmp_path / "with_missing.tex"
    base = (Path(__file__).resolve().parents[1] / "fixtures" / "sample.tex").read_text(
        encoding="utf-8"
    )
    tex.write_text(
        base.replace(
            r"\end{itemize}",
            r"""\item \textbf{Phantom Paper Without Bib Match}\cite{NotInBib2026}:

A cite that has no matching entry in the bib file.
\end{itemize}""",
        ),
        encoding="utf-8",
    )

    with caplog.at_level(logging.WARNING, logger="paper_sorts.services.import_service"):
        _invoke(
            fresh_engine,
            tex,
            Path(__file__).resolve().parents[1] / "fixtures" / "sample.bib",
        )

    with Session(bind=fresh_engine) as session:
        keys = set(session.execute(select(Paper.bibtex_id)).scalars())
    assert keys == {
        "Lee2022DirectSpeechToSpeech",
        "Wang2021LargeScaleSA",
        "Schoettler2023FairnessMT",
    }

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("NotInBib2026" in r.getMessage() for r in warnings)

    out = capsys.readouterr().out
    assert "inserted=3" in out
    assert "warned=1" in out


def test_idempotent_rerun_skips_all(
    fresh_engine: Engine, capsys: pytest.CaptureFixture[str]
) -> None:
    tex = Path(__file__).resolve().parents[1] / "fixtures" / "sample.tex"
    bib = Path(__file__).resolve().parents[1] / "fixtures" / "sample.bib"
    _invoke(fresh_engine, tex, bib)
    capsys.readouterr()  # discard first-run output
    _invoke(fresh_engine, tex, bib)

    out = capsys.readouterr().out
    assert "inserted=0" in out
    assert "skipped=3" in out

    with Session(bind=fresh_engine) as session:
        paper_count = len(list(session.execute(select(Paper.id)).scalars()))
        bib_count = len(list(session.execute(select(BibEntry.bibtex_id)).scalars()))
        author_count = len(list(session.execute(select(Author.id)).scalars()))
    assert paper_count == 3
    assert bib_count == 3
    # Lee, Pino, Wang, Schoettler — four distinct authors across the three papers.
    assert author_count == 4


def test_partial_failure_persists_completed_entries(
    fresh_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_add = paper_service.PaperService.add_paper
    call_count = {"n": 0}

    def flaky_add(self: paper_service.PaperService, payload: object) -> object:
        call_count["n"] += 1
        if call_count["n"] == 3:
            msg = "injected failure on third entry"
            raise RuntimeError(msg)
        return real_add(self, payload)  # type: ignore[arg-type]

    monkeypatch.setattr(paper_service.PaperService, "add_paper", flaky_add)

    with pytest.raises(RuntimeError, match="injected failure"):
        _invoke(
            fresh_engine,
            Path(__file__).resolve().parents[1] / "fixtures" / "sample.tex",
            Path(__file__).resolve().parents[1] / "fixtures" / "sample.bib",
        )

    with Session(bind=fresh_engine) as session:
        keys = set(session.execute(select(Paper.bibtex_id)).scalars())
    # First two entries persisted; the third was rolled back by with_session.
    assert keys == {"Lee2022DirectSpeechToSpeech", "Wang2021LargeScaleSA"}
