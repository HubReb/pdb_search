"""Baseline benchmark harness for paper_sorts interactive operations.

Measures wall-clock time for: search-by-title, search-by-author, add, update, delete.
Records results to tests/benchmarks/baseline.json.

This benchmark MUST execute successfully — it must NOT be permanently skipped
(constitution Principle IV baseline-benchmark gate). The recorded times establish
the baseline for the non-regression criterion.

Run standalone:
    uv run pytest tests/benchmarks/bench_baseline.py -v

Or as part of the full suite (included by default via testpaths = ["tests"]).
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from paper_sorts.db.repositories import PaperCreate
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service
from tests.fixtures.seed_papers import PAPER_1, PAPER_2

_BASELINE_FILE = Path(__file__).resolve().parent / "baseline.json"

# Acceptable upper bound: 5 seconds per operation on a personal-library-sized dataset.
# This is a generous bound — actual times on commodity hardware should be < 1s.
# The bound exists to catch catastrophic regressions, not to mandate specific performance.
_MAX_SECONDS_PER_OP = 5.0

_BENCH_PAPER = PaperCreate(
    title="Benchmark Test Paper",
    contents="Benchmark summary.",
    bibtex_id="BenchTest2026",
    bibtex="@misc{BenchTest2026, title={Benchmark}}",
    authors=["Bench, Mark"],
)


def _time_operation(fn: Any, *args: Any, **kwargs: Any) -> float:  # noqa: ANN401
    """Time a single operation in seconds.

    :param fn: Callable to time.
    :param args: Positional arguments.
    :param kwargs: Keyword arguments.
    :return: Wall-clock time in seconds.
    """
    start = time.perf_counter()
    fn(*args, **kwargs)
    return time.perf_counter() - start


class TestBaslineBenchmarks:
    """Wall-clock benchmarks for all five interactive operations.

    These tests execute and record real timings — they are NOT skipped.
    Results are written to baseline.json after each run.
    """

    def test_bench_search_by_title(self, ephemeral_db_url: str, db_session: object) -> None:
        """Benchmark search-by-title. Must complete in under _MAX_SECONDS_PER_OP."""
        with with_session(ephemeral_db_url) as session:
            elapsed = _time_operation(
                paper_service.search_by_title, session, PAPER_1.title
            )
        assert elapsed < _MAX_SECONDS_PER_OP, (
            f"search_by_title took {elapsed:.3f}s, exceeds {_MAX_SECONDS_PER_OP}s bound"
        )
        _record_result("search_by_title", elapsed)

    def test_bench_search_by_author(self, ephemeral_db_url: str, db_session: object) -> None:
        """Benchmark search-by-author. Must complete in under _MAX_SECONDS_PER_OP."""
        with with_session(ephemeral_db_url) as session:
            elapsed = _time_operation(
                paper_service.search_by_author, session, PAPER_2.authors[0]
            )
        assert elapsed < _MAX_SECONDS_PER_OP, (
            f"search_by_author took {elapsed:.3f}s, exceeds {_MAX_SECONDS_PER_OP}s bound"
        )
        _record_result("search_by_author", elapsed)

    def test_bench_add(self, ephemeral_db_url: str, db_session: object) -> None:
        """Benchmark add paper. Must complete in under _MAX_SECONDS_PER_OP."""
        # Ensure clean state
        with with_session(ephemeral_db_url) as session:
            paper_service.delete_paper(session, _BENCH_PAPER.bibtex_id)

        with with_session(ephemeral_db_url) as session:
            elapsed = _time_operation(paper_service.add_paper, session, _BENCH_PAPER)
        assert elapsed < _MAX_SECONDS_PER_OP, (
            f"add_paper took {elapsed:.3f}s, exceeds {_MAX_SECONDS_PER_OP}s bound"
        )
        _record_result("add_paper", elapsed)

    def test_bench_update(self, ephemeral_db_url: str, db_session: object) -> None:
        """Benchmark update field. Must complete in under _MAX_SECONDS_PER_OP."""
        # Ensure the benchmark paper exists
        with with_session(ephemeral_db_url) as session:
            paper_service.add_paper(session, _BENCH_PAPER)

        with with_session(ephemeral_db_url) as session:
            elapsed = _time_operation(
                paper_service.update_field,
                session,
                "papers",
                "contents",
                _BENCH_PAPER.title,
                "Updated benchmark summary.",
            )
        assert elapsed < _MAX_SECONDS_PER_OP, (
            f"update_field took {elapsed:.3f}s, exceeds {_MAX_SECONDS_PER_OP}s bound"
        )
        _record_result("update_field", elapsed)

    def test_bench_delete(self, ephemeral_db_url: str, db_session: object) -> None:
        """Benchmark delete paper. Must complete in under _MAX_SECONDS_PER_OP."""
        # Ensure the benchmark paper exists
        with with_session(ephemeral_db_url) as session:
            paper_service.add_paper(session, _BENCH_PAPER)

        with with_session(ephemeral_db_url) as session:
            elapsed = _time_operation(
                paper_service.delete_paper, session, _BENCH_PAPER.bibtex_id
            )
        assert elapsed < _MAX_SECONDS_PER_OP, (
            f"delete_paper took {elapsed:.3f}s, exceeds {_MAX_SECONDS_PER_OP}s bound"
        )
        _record_result("delete_paper", elapsed)


def _record_result(operation: str, elapsed: float) -> None:
    """Append or update a benchmark result in baseline.json.

    :param operation: Name of the benchmarked operation.
    :param elapsed: Wall-clock time in seconds.
    """
    if _BASELINE_FILE.exists():
        try:
            data = json.loads(_BASELINE_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            data = {}
    else:
        data = {}

    data[operation] = {"seconds": round(elapsed, 6), "bound": _MAX_SECONDS_PER_OP}
    _BASELINE_FILE.write_text(json.dumps(data, indent=2))
