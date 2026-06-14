"""Baseline benchmark harness for paper_sorts.

Constitution Principle IV, Gate G2: this benchmark MUST execute and record
results to baseline.json.  It MUST NOT be permanently @pytest.mark.skip'd.

Records wall-clock timings (in seconds) for:
  - search_by_title
  - search_by_author
  - add_paper
  - update_field
  - delete_paper

Run: pytest tests/benchmarks/ -m benchmark -v
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from paper_sorts.db.repositories import PaperCreate
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service

_BASELINE_FILE = Path(__file__).parent / "baseline.json"
_ITERATIONS = 5  # Repeat each operation to get a more stable reading

# Sample paper for benchmark use
_BENCH_PAPER = PaperCreate(
    title="Benchmark Paper on Large Language Models",
    contents="This paper evaluates large language models across various benchmarks.",
    bibtex_id="Bench2024LLM",
    bibtex="@article{Bench2024LLM, title={Benchmark Paper}, author={Bench, Mark}, year={2024}}",
    authors=["Bench, Mark"],
)


@pytest.fixture(scope="module")
def bench_db_url(postgresql_proc: object) -> str:  # type: ignore[type-arg]
    """Set up a dedicated benchmark database with seed data.

    Args:
        postgresql_proc: The ephemeral PostgreSQL process fixture.

    Returns:
        SQLAlchemy connection URL for the benchmark DB.
    """

    from sqlalchemy import create_engine, text

    proc = postgresql_proc
    bench_dbname = "paper_sorts_bench"
    admin_url = (
        f"postgresql+psycopg://{proc.user}:@"  # type: ignore[union-attr]
        f"{proc.host}:{proc.port}/postgres"  # type: ignore[union-attr]
    )
    engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        conn.execute(text(f"DROP DATABASE IF EXISTS {bench_dbname}"))
        conn.execute(text(f"CREATE DATABASE {bench_dbname}"))
    engine.dispose()

    url = (
        f"postgresql+psycopg://{proc.user}:@"  # type: ignore[union-attr]
        f"{proc.host}:{proc.port}/{bench_dbname}"  # type: ignore[union-attr]
    )

    from alembic import command
    from alembic.config import Config

    alembic_cfg = Config("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", url)
    command.upgrade(alembic_cfg, "head")

    # Seed with a modest set of papers for benchmark realism
    from tests.fixtures.seed_papers import SEED_PAPERS

    with with_session(url) as session:
        for paper in SEED_PAPERS:
            paper_service.add_paper(session, paper)

    return url


def _time_op(fn: object, *args: object, iterations: int = _ITERATIONS) -> float:
    """Measure average wall-clock time of an operation.

    Args:
        fn: Callable to benchmark.
        *args: Arguments to pass to fn.
        iterations: Number of times to run fn.

    Returns:
        Average wall-clock time in seconds.
    """
    import typing

    callable_fn = typing.cast(typing.Callable[..., object], fn)
    times: list[float] = []
    for _ in range(iterations):
        start = time.perf_counter()
        callable_fn(*args)
        elapsed = time.perf_counter() - start
        times.append(elapsed)
    return sum(times) / len(times)


@pytest.mark.benchmark
def test_benchmark_search_by_title(bench_db_url: str) -> None:
    """Benchmark search_by_title operation and record baseline."""

    def _op() -> None:
        with with_session(bench_db_url) as session:
            paper_service.search_by_title(session, "Attention")

    avg = _time_op(_op)
    _record_result("search_by_title", avg)
    assert avg < 5.0, f"search_by_title too slow: {avg:.3f}s"


@pytest.mark.benchmark
def test_benchmark_search_by_author(bench_db_url: str) -> None:
    """Benchmark search_by_author operation and record baseline."""

    def _op() -> None:
        with with_session(bench_db_url) as session:
            paper_service.search_by_author(session, "Vaswani")

    avg = _time_op(_op)
    _record_result("search_by_author", avg)
    assert avg < 5.0, f"search_by_author too slow: {avg:.3f}s"


@pytest.mark.benchmark
def test_benchmark_add_paper(bench_db_url: str) -> None:
    """Benchmark add_paper operation and record baseline."""
    counter = [0]

    def _op() -> None:
        idx = counter[0]
        counter[0] += 1
        p = PaperCreate(
            title=f"Bench Add Paper {idx}",
            contents="Benchmark content.",
            bibtex_id=f"BenchAdd{idx}",
            bibtex=f"@article{{BenchAdd{idx}, title={{Bench}}, year={{2024}}}}",
            authors=["Bench, Mark"],
        )
        with with_session(bench_db_url) as session:
            paper_service.add_paper(session, p)

    avg = _time_op(_op)
    _record_result("add_paper", avg)
    assert avg < 5.0, f"add_paper too slow: {avg:.3f}s"


@pytest.mark.benchmark
def test_benchmark_update_field(bench_db_url: str) -> None:
    """Benchmark update_field (title) operation and record baseline."""
    # Find a paper to update
    with with_session(bench_db_url) as session:
        results = paper_service.search_by_title(session, "Attention Is All You Need")
    assert results, "Need at least one paper for update benchmark"
    paper_id = results[0].id

    counter = [0]

    def _op() -> None:
        counter[0] += 1
        with with_session(bench_db_url) as session:
            paper_service.update_field(session, paper_id, "contents", f"Updated contents {counter[0]}")

    avg = _time_op(_op)
    _record_result("update_field", avg)
    assert avg < 5.0, f"update_field too slow: {avg:.3f}s"


@pytest.mark.benchmark
def test_benchmark_delete_paper(bench_db_url: str) -> None:
    """Benchmark delete_paper operation and record baseline."""
    # Add papers to delete
    paper_ids: list[int] = []
    for i in range(_ITERATIONS):
        p = PaperCreate(
            title=f"Bench Delete Paper {i}",
            contents="To be deleted.",
            bibtex_id=f"BenchDel{i}",
            bibtex=f"@article{{BenchDel{i}, title={{Del}}, year={{2024}}}}",
            authors=["Bench, Mark"],
        )
        with with_session(bench_db_url) as session:
            result = paper_service.add_paper(session, p)
            paper_ids.append(result.id)

    iter_ids = iter(paper_ids)

    def _op() -> None:
        pid = next(iter_ids)
        with with_session(bench_db_url) as session:
            paper_service.delete_paper(session, pid)

    avg = _time_op(_op)
    _record_result("delete_paper", avg)
    assert avg < 5.0, f"delete_paper too slow: {avg:.3f}s"


def _record_result(operation: str, avg_seconds: float) -> None:
    """Append a benchmark result to baseline.json.

    Args:
        operation: Name of the benchmarked operation.
        avg_seconds: Average wall-clock time in seconds.
    """
    if _BASELINE_FILE.exists():
        data: dict[str, float] = json.loads(_BASELINE_FILE.read_text())
    else:
        data = {}
    data[operation] = round(avg_seconds, 6)
    _BASELINE_FILE.write_text(json.dumps(data, indent=2))
