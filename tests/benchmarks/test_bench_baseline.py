"""Baseline benchmark harness for paper_sorts interactive operations.

Measures wall-clock time for: search_by_title (single match), search_by_title
(multi-match), search_by_author, add_paper, update_field, delete_paper.

Records results to tests/benchmarks/baseline.json.

Constitution Principle IV — Baseline-benchmark gate:
  This harness MUST NOT be permanently @pytest.mark.skip'd.
  Run via: uv run pytest tests/benchmarks/bench_baseline.py -v
  Or for recorded baseline: uv run pytest tests/benchmarks/ --benchmark-json=tests/benchmarks/baseline.json
"""

import pathlib

import pytest
from sqlalchemy.orm import Session

from paper_sorts.db.repositories import PaperCreate, PaperRepository
from paper_sorts.services import paper_service

BASELINE_FILE = pathlib.Path(__file__).parent / "baseline.json"


# ---------------------------------------------------------------------------
# Benchmark fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def bench_session(db_engine):  # type: ignore[no-untyped-def]
    """Yield a Session with a known set of papers for benchmarking.

    Seeds 4 papers matching the standard SEED_PAPERS set.
    Uses rollback teardown so benchmarks don't pollute each other.
    """
    from sqlalchemy.orm import Session as _Session

    from tests.fixtures.seed_papers import SEED_PAPERS

    connection = db_engine.connect()
    transaction = connection.begin()
    session = _Session(bind=connection)
    repo = PaperRepository(session)
    for paper_data in SEED_PAPERS:
        try:
            repo.add(PaperCreate(**paper_data))
        except ValueError:
            pass
    session.flush()
    yield session
    session.close()
    transaction.rollback()
    connection.close()


# ---------------------------------------------------------------------------
# Benchmark tests (using pytest-benchmark)
# ---------------------------------------------------------------------------


def test_bench_search_by_title_single(benchmark, bench_session: Session) -> None:  # type: ignore[no-untyped-def]
    """Benchmark: search_by_title returning a single result (BERT)."""
    result = benchmark(paper_service.search_by_title, bench_session, "BERT")
    assert len(result) >= 1


def test_bench_search_by_title_multi(benchmark, bench_session: Session) -> None:  # type: ignore[no-untyped-def]
    """Benchmark: search_by_title returning multiple results (Attention)."""
    result = benchmark(paper_service.search_by_title, bench_session, "Attention")
    assert len(result) >= 2


def test_bench_search_by_author(benchmark, bench_session: Session) -> None:  # type: ignore[no-untyped-def]
    """Benchmark: search_by_author returning a result (Vaswani)."""
    result = benchmark(paper_service.search_by_author, bench_session, "Vaswani")
    assert len(result) >= 1


def test_bench_add_paper(benchmark, bench_session: Session) -> None:  # type: ignore[no-untyped-def]
    """Benchmark: add_paper for a new entry."""
    counter = {"n": 0}

    def add_and_cleanup() -> None:
        counter["n"] += 1
        key = f"BenchPaper{counter['n']:05d}"
        data = PaperCreate(
            title=f"Benchmark Paper {counter['n']}",
            contents="Benchmark abstract.",
            bibtex_id=key,
            bibtex=f"@article{{{key}}}",
            authors=["Bench, Mark"],
        )
        paper_service.add_paper(bench_session, data)
        bench_session.flush()

    benchmark(add_and_cleanup)


def test_bench_update_field(benchmark, bench_session: Session) -> None:  # type: ignore[no-untyped-def]
    """Benchmark: update_field (title) on an existing paper."""
    results = paper_service.search_by_title(bench_session, "BERT")
    paper_id = results[0].id

    counter = {"n": 0}

    def update() -> None:
        counter["n"] += 1
        paper_service.update_field(bench_session, paper_id, "title", f"BERT v{counter['n']}")
        bench_session.flush()

    benchmark(update)


def test_bench_delete_paper(benchmark, bench_session: Session) -> None:  # type: ignore[no-untyped-def]
    """Benchmark: delete_paper (add then delete to keep state consistent)."""
    counter = {"n": 0}

    def add_then_delete() -> None:
        counter["n"] += 1
        key = f"DelBench{counter['n']:05d}"
        data = PaperCreate(
            title=f"Delete Bench {counter['n']}",
            contents="To be deleted.",
            bibtex_id=key,
            bibtex=f"@article{{{key}}}",
            authors=[],
        )
        paper = paper_service.add_paper(bench_session, data)
        bench_session.flush()
        paper_service.delete_paper(bench_session, paper.id)
        bench_session.flush()

    benchmark(add_then_delete)
