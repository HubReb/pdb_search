"""Benchmark harness for paper_sorts interactive operations.

Records wall-clock times for the five interactive operations:
  - search_by_title
  - search_by_author
  - add_paper
  - update_field
  - delete_paper

On first run, results are written to tests/benchmarks/baseline.json.
On subsequent runs, each operation is asserted to be within 2x of the
recorded baseline (constitution Principle IV, Gate G2).

This harness MUST NOT be permanently @pytest.mark.skip'd.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

from paper_sorts.db.repositories import PaperCreate
from paper_sorts.services import paper_service

_BASELINE_FILE = Path(__file__).parent / "baseline.json"
_MULTIPLIER = 2.0  # Allow up to 2x the baseline


# ---------------------------------------------------------------------------
# Timing helpers
# ---------------------------------------------------------------------------


def _time_operation(fn, *args, **kwargs) -> float:  # type: ignore[no-untyped-def]
    """Return the wall-clock seconds taken to call fn(*args, **kwargs)."""
    start = time.perf_counter()
    fn(*args, **kwargs)
    return time.perf_counter() - start


def _load_baseline() -> dict[str, float] | None:
    """Load the baseline timings, or return None if not yet recorded."""
    if not _BASELINE_FILE.exists():
        return None
    with open(_BASELINE_FILE) as f:
        return json.load(f)


def _save_baseline(timings: dict[str, float]) -> None:
    """Write timings to the baseline file."""
    _BASELINE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(_BASELINE_FILE, "w") as f:
        json.dump(timings, f, indent=2)


# ---------------------------------------------------------------------------
# Benchmark tests
# ---------------------------------------------------------------------------


class TestBenchmarkBaseline:
    """Benchmark harness for all five interactive operations.

    On first run: records baseline.
    On subsequent runs: asserts no operation takes more than 2x the baseline.
    """

    def test_search_by_title(self, seeded_db_url: str) -> None:
        """Benchmark: search_by_title on seeded data."""
        elapsed = _time_operation(
            paper_service.search_by_title,
            seeded_db_url,
            "Large-scale Self- and Semi-Supervised Learning for Speech Translation",
        )
        _assert_or_record("search_by_title", elapsed)

    def test_search_by_author(self, seeded_db_url: str) -> None:
        """Benchmark: search_by_author on seeded data."""
        elapsed = _time_operation(
            paper_service.search_by_author,
            seeded_db_url,
            "Pino, J.",
        )
        _assert_or_record("search_by_author", elapsed)

    def test_add_paper(self, clean_db_session: object, ephemeral_db_url: str) -> None:
        """Benchmark: add_paper with one author."""
        paper = PaperCreate(
            title="Benchmark Add Paper",
            contents="Benchmark summary.",
            bibtex_id="BenchAdd2026",
            bibtex="@misc{BenchAdd2026}",
            authors=["Bench, Mark"],
        )
        elapsed = _time_operation(paper_service.add_paper, ephemeral_db_url, paper)
        _assert_or_record("add_paper", elapsed)

    def test_update_field(self, clean_db_session: object, ephemeral_db_url: str) -> None:
        """Benchmark: update_field (title update)."""
        paper = PaperCreate(
            title="Bench Update Title",
            contents="Summary.",
            bibtex_id="BenchUpdate2026",
            bibtex="@misc{BenchUpdate2026}",
            authors=["Bench, Mark"],
        )
        paper_service.add_paper(ephemeral_db_url, paper)
        results = paper_service.search_by_title(ephemeral_db_url, "Bench Update Title")
        paper_id = results[0].paper_id

        elapsed = _time_operation(
            paper_service.update_field,
            ephemeral_db_url,
            "papers",
            "title",
            paper_id,
            "Bench Update New Title",
        )
        _assert_or_record("update_field", elapsed)

    def test_delete_paper(self, clean_db_session: object, ephemeral_db_url: str) -> None:
        """Benchmark: delete_paper."""
        paper = PaperCreate(
            title="Bench Delete Paper",
            contents="Summary.",
            bibtex_id="BenchDelete2026",
            bibtex="@misc{BenchDelete2026}",
            authors=["Bench, Mark"],
        )
        paper_service.add_paper(ephemeral_db_url, paper)
        results = paper_service.search_by_title(ephemeral_db_url, "Bench Delete Paper")
        paper_id = results[0].paper_id

        elapsed = _time_operation(paper_service.delete_paper, ephemeral_db_url, paper_id)
        _assert_or_record("delete_paper", elapsed)


# ---------------------------------------------------------------------------
# Module-level baseline accumulator
# ---------------------------------------------------------------------------

_new_timings: dict[str, float] = {}


def _assert_or_record(operation: str, elapsed: float) -> None:
    """Assert elapsed is within 2x baseline, or record it as new baseline.

    Args:
        operation: Name of the operation (key in baseline.json).
        elapsed: Measured wall-clock time in seconds.
    """
    baseline = _load_baseline()
    _new_timings[operation] = elapsed

    if baseline is None:
        # First run — save and pass
        _save_baseline({**_new_timings})
        return

    if operation not in baseline:
        # New operation — record and pass
        baseline[operation] = elapsed
        _save_baseline(baseline)
        return

    recorded = baseline[operation]
    limit = recorded * _MULTIPLIER
    assert elapsed <= limit, (
        f"Performance regression detected: {operation} took {elapsed:.3f}s "
        f"(baseline: {recorded:.3f}s, limit: {limit:.3f}s). "
        "Consider re-recording the baseline if this is an expected improvement: "
        "delete tests/benchmarks/baseline.json and re-run."
    )
