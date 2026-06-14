"""Baseline benchmark harness for paper_sorts.

Measures wall-clock time for the five interactive operations mandated by the
project constitution (Principle IV baseline-benchmark gate):
  - search_by_title
  - search_by_author
  - add_paper
  - update_field
  - delete_paper

Results are written to ``tests/benchmarks/baseline.json``.

This benchmark MUST NOT be permanently ``@pytest.mark.skip``'d — the
constitution requires it to execute successfully.  Run via:

    uv run pytest tests/benchmarks/ -v

or directly:

    python tests/benchmarks/bench_baseline.py
"""

from __future__ import annotations

import json
import timeit
from pathlib import Path

from paper_sorts.db.repositories import PaperCreate
from paper_sorts.services import paper_service

BASELINE_JSON = Path(__file__).parent / "baseline.json"

# Number of timing repetitions (kept low to avoid slow CI)
REPEATS = 3


def _seed_one(engine: object, idx: int = 0) -> int:
    """Add a benchmark paper and return its id."""
    paper = PaperCreate(
        title=f"Benchmark Paper {idx}",
        contents="Benchmark summary.",
        bibtex_id=f"BenchKey{idx}",
        bibtex=f"@article{{BenchKey{idx}, year={{2024}}}}",
        authors=["Bench, Author"],
    )
    result = paper_service.add_paper(engine, paper)  # type: ignore[arg-type]
    return result.id


class TestBaseline:
    """Benchmark tests that record wall-clock baseline timings.

    Each test measures a single operation, records the median wall-clock time
    in seconds, and writes the full results dict to baseline.json.
    """

    results: dict[str, float] = {}

    def _time_it(self, fn: object, number: int = REPEATS) -> float:
        """Return mean wall-clock time in seconds over *number* runs."""
        elapsed = timeit.timeit(fn, number=number)  # type: ignore[arg-type]
        return elapsed / number

    def test_bench_search_by_title(self, seeded_engine: object) -> None:
        """Benchmark search_by_title."""
        title = "Direct speech-to-speech translation with discrete units"
        mean = self._time_it(
            lambda: paper_service.search_by_title(seeded_engine, title)  # type: ignore[arg-type]
        )
        TestBaseline.results["search_by_title_s"] = mean
        # Sanity: must return at least one result
        results = paper_service.search_by_title(seeded_engine, title)  # type: ignore[arg-type]
        assert len(results) >= 1
        print(f"search_by_title: {mean * 1000:.2f} ms mean over {REPEATS} runs")

    def test_bench_search_by_author(self, seeded_engine: object) -> None:
        """Benchmark search_by_author."""
        mean = self._time_it(
            lambda: paper_service.search_by_author(seeded_engine, "Pino, J.")  # type: ignore[arg-type]
        )
        TestBaseline.results["search_by_author_s"] = mean
        print(f"search_by_author: {mean * 1000:.2f} ms mean over {REPEATS} runs")

    def test_bench_add_and_delete(self, engine: object) -> None:
        """Benchmark add_paper and delete_paper (measured together)."""
        paper_ids: list[int] = []

        def do_add() -> None:
            idx = len(paper_ids)
            pid = _seed_one(engine, 1000 + idx)
            paper_ids.append(pid)

        add_mean = self._time_it(do_add)
        TestBaseline.results["add_paper_s"] = add_mean
        print(f"add_paper: {add_mean * 1000:.2f} ms mean over {REPEATS} runs")

        def do_delete() -> None:
            if paper_ids:
                pid = paper_ids.pop()
                paper_service.delete_paper(engine, pid)  # type: ignore[arg-type]

        del_mean = self._time_it(do_delete)
        TestBaseline.results["delete_paper_s"] = del_mean
        print(f"delete_paper: {del_mean * 1000:.2f} ms mean over {REPEATS} runs")

        # Cleanup any remaining
        for pid in paper_ids:
            try:
                paper_service.delete_paper(engine, pid)  # type: ignore[arg-type]
            except Exception:
                pass

    def test_bench_update_field(self, engine: object) -> None:
        """Benchmark update_field."""
        pid = _seed_one(engine, 9000)
        try:
            mean = self._time_it(
                lambda: paper_service.update_field(
                    engine, pid, "papers", "contents", "Updated bench summary."  # type: ignore[arg-type]
                )
            )
            TestBaseline.results["update_field_s"] = mean
            print(f"update_field: {mean * 1000:.2f} ms mean over {REPEATS} runs")
        finally:
            try:
                paper_service.delete_paper(engine, pid)  # type: ignore[arg-type]
            except Exception:
                pass

    def test_write_baseline_json(self) -> None:
        """Write accumulated timing results to baseline.json."""
        # This test always runs last (alphabetically after the bench_ tests).
        # Results may be partial if earlier tests failed.
        if TestBaseline.results:
            BASELINE_JSON.write_text(
                json.dumps(TestBaseline.results, indent=2) + "\n",
                encoding="utf-8",
            )
            print(f"Baseline written to {BASELINE_JSON}")
        assert True  # always pass; the gate is "executes", not "is fast"
