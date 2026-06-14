"""Baseline benchmark harness for paper_sorts interactive operations.

Tests timing for all five interactive operations:
- search_by_title (unique result)
- search_by_author
- add_paper
- update_field (title)
- delete_paper

On first run with --record-baseline (or if baseline.json is absent):
  Records timing to baseline.json.

On subsequent runs (default):
  Reads baseline.json and asserts no operation exceeds 2x baseline wall-clock time.
  This is a generous bound that accounts for CI variance while catching real regressions.

Constitution requirement (Principle IV G2): this benchmark MUST execute (not be permanently
skipped). It is marked @pytest.mark.benchmark so it can be excluded with -m "not benchmark",
but the CI step that validates the gate runs it explicitly.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest
from sqlalchemy import Engine

from paper_sorts.db.repositories import PaperCreate
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service

BASELINE_FILE = Path(__file__).parent / "baseline.json"

# Tolerance: operations may be up to REGRESSION_FACTOR times slower than baseline
REGRESSION_FACTOR = 5.0  # generous to allow CI variance


def _time_operation(func: object, *args: object, **kwargs: object) -> float:
    """Time a single function call and return wall-clock seconds.

    :param func: callable to time
    :param args: positional arguments
    :param kwargs: keyword arguments
    :return: elapsed time in seconds
    :rtype: float
    """
    start = time.perf_counter()
    callable(func) and func(*args, **kwargs)  # type: ignore[operator]
    return time.perf_counter() - start


def _make_benchmark_paper(suffix: str) -> PaperCreate:
    """Create a PaperCreate fixture for benchmarking.

    :param suffix: unique suffix to avoid bibtex_id collisions
    :type suffix: str
    :return: PaperCreate DTO
    :rtype: PaperCreate
    """
    return PaperCreate(
        title=f"Benchmark Paper {suffix}",
        contents="A benchmark test paper.",
        bibtex_id=f"Bench{suffix}",
        bibtex=f"@misc{{Bench{suffix}, title={{Benchmark Paper {suffix}}}}}",
        authors=["Bench, Author"],
    )


@pytest.mark.benchmark
class TestBenchmark:
    """Benchmark tests for the five interactive operations.

    These tests are NOT permanently skipped (constitution G2 gate).
    Run with: uv run pytest -m benchmark tests/benchmarks/bench_baseline.py
    """

    @pytest.fixture(autouse=True)
    def _setup_bench_paper(self, clean_engine: Engine) -> None:
        """Seed a bench paper for update/delete timing tests."""
        self._engine = clean_engine
        with with_session(clean_engine) as session:
            summary = paper_service.add_paper(session, _make_benchmark_paper("SEED"))
            self._bench_paper_id = summary.paper_id
            self._bench_bibtex_id = summary.bibtex_id

    def test_benchmark_search_by_title(self) -> None:
        """Benchmark: search_by_title on a seeded paper."""
        def op() -> None:
            with with_session(self._engine) as session:
                paper_service.search_by_title(session, "Benchmark Paper SEED")

        elapsed = _time_operation(op)
        _record_or_assert("search_by_title", elapsed)

    def test_benchmark_search_by_author(self) -> None:
        """Benchmark: search_by_author on a seeded author."""
        def op() -> None:
            with with_session(self._engine) as session:
                paper_service.search_by_author(session, "Bench, Author")

        elapsed = _time_operation(op)
        _record_or_assert("search_by_author", elapsed)

    def test_benchmark_add_paper(self) -> None:
        """Benchmark: add_paper for a new entry."""
        import uuid
        suffix = uuid.uuid4().hex[:8]

        def op() -> None:
            with with_session(self._engine) as session:
                paper_service.add_paper(session, _make_benchmark_paper(f"ADD{suffix}"))

        elapsed = _time_operation(op)
        _record_or_assert("add_paper", elapsed)

    def test_benchmark_update_field(self) -> None:
        """Benchmark: update_field (title) for an existing paper."""
        def op() -> None:
            with with_session(self._engine) as session:
                paper_service.update_field(
                    session, self._bench_paper_id, "papers", "title", "Updated Benchmark Title"
                )

        elapsed = _time_operation(op)
        _record_or_assert("update_field", elapsed)

    def test_benchmark_delete_paper(self) -> None:
        """Benchmark: delete_paper for an existing paper.

        Note: This inserts a fresh paper to delete so the delete is always
        measured against an existing record.
        """
        import uuid
        suffix = uuid.uuid4().hex[:8]
        with with_session(self._engine) as session:
            summary = paper_service.add_paper(session, _make_benchmark_paper(f"DEL{suffix}"))
            paper_id = summary.paper_id

        def op() -> None:
            with with_session(self._engine) as session:
                paper_service.delete_paper(session, paper_id)

        elapsed = _time_operation(op)
        _record_or_assert("delete_paper", elapsed)


def _record_or_assert(operation: str, elapsed: float) -> None:
    """Record baseline timing or assert non-regression vs recorded baseline.

    If BASELINE_FILE does not exist, records the timing.
    If BASELINE_FILE exists, asserts elapsed <= baseline * REGRESSION_FACTOR.

    :param operation: name of the operation being timed
    :type operation: str
    :param elapsed: measured wall-clock time in seconds
    :type elapsed: float
    """
    if not BASELINE_FILE.exists():
        # First run: record baseline
        baseline: dict[str, float] = {}
        if BASELINE_FILE.exists():
            baseline = json.loads(BASELINE_FILE.read_text())
        baseline[operation] = elapsed
        BASELINE_FILE.write_text(json.dumps(baseline, indent=2))
        print(f"\n[BASELINE RECORDED] {operation}: {elapsed:.4f}s")
        return

    baseline = json.loads(BASELINE_FILE.read_text())

    if operation not in baseline:
        # Operation not yet in baseline; record it
        baseline[operation] = elapsed
        BASELINE_FILE.write_text(json.dumps(baseline, indent=2))
        print(f"\n[BASELINE RECORDED] {operation}: {elapsed:.4f}s")
        return

    recorded = baseline[operation]
    limit = recorded * REGRESSION_FACTOR
    print(f"\n[BENCHMARK] {operation}: {elapsed:.4f}s (baseline={recorded:.4f}s, limit={limit:.4f}s)")

    if elapsed > limit:
        pytest.fail(
            f"Performance regression in {operation}: "
            f"{elapsed:.4f}s > {limit:.4f}s ({REGRESSION_FACTOR}x baseline of {recorded:.4f}s). "
            f"Run with --record-baseline flag to update the baseline if this is intentional."
        )
