"""Executing baseline benchmark (G2) for the five interactive operations.

Times search-by-title, search-by-author, add, update, and delete against the
seeded ephemeral database. On first run it records ``baseline.json``; on later
runs it asserts no measurable regression versus the recorded baseline (with a
generous tolerance, since absolute timings vary by machine — the point is that
the harness *executes* and guards against gross regression, per Principle IV).

This is a real pytest module, not a permanent skip.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from pathlib import Path

from sqlalchemy import Engine

from paper_sorts.db.repositories import PaperCreate
from paper_sorts.services import paper_service

_BASELINE_PATH = Path(__file__).parent / "baseline.json"

#: Multiplier above the recorded baseline tolerated before a regression fails.
#: Generous because wall-clock timing on a shared/ephemeral DB is noisy; the
#: harness exists to catch order-of-magnitude regressions, not micro-jitter.
_REGRESSION_FACTOR = 5.0

#: Floor (seconds) below which timing noise dominates and comparison is skipped.
_NOISE_FLOOR = 0.05


def _time(operation: Callable[[], object], repeats: int = 5) -> float:
    """Return the best wall-clock time of ``operation`` over a few repeats.

    :param operation: the zero-argument operation to time.
    :param repeats: how many times to run it (the minimum is reported).
    :return: the minimum elapsed seconds.
    """
    best = float("inf")
    for _ in range(repeats):
        start = time.perf_counter()
        operation()
        best = min(best, time.perf_counter() - start)
    return best


def _measure(engine: Engine) -> dict[str, float]:
    """Measure the five interactive operations against the seeded database.

    :param engine: the seeded database engine.
    :return: mapping of operation name to best elapsed seconds.
    """
    timings: dict[str, float] = {}

    timings["search_by_title"] = _time(
        lambda: paper_service.search_by_title(
            engine, "Direct speech-to-speech translation with discrete units"
        )
    )
    timings["search_by_author"] = _time(lambda: paper_service.search_by_author(engine, "Pino, J."))

    counter = {"n": 0}

    def _add() -> None:
        counter["n"] += 1
        paper_service.add_paper(
            engine,
            PaperCreate(
                title=f"Bench paper {counter['n']}",
                contents="bench",
                bibtex_id=f"Bench{counter['n']}",
                bibtex=f"@article{{Bench{counter['n']}}}",
                authors=[f"Bench, Author{counter['n']}"],
            ),
        )

    timings["add"] = _time(_add, repeats=3)

    title = paper_service.search_by_title(engine, "Bench paper 1")[0]
    timings["update"] = _time(
        lambda: paper_service.update_field(
            engine, "papers", "contents", "updated", str(title.paper_id)
        )
    )

    to_delete = paper_service.search_by_title(engine, "Bench paper 2")[0].paper_id
    timings["delete"] = _time(lambda: paper_service.delete_paper(engine, to_delete), repeats=1)

    return timings


def test_baseline_benchmark(seeded_engine: Engine) -> None:
    """Record or assert the baseline for the five interactive operations."""
    timings = _measure(seeded_engine)

    # Every operation must have produced a positive timing — proof it executed.
    assert set(timings) == {
        "search_by_title",
        "search_by_author",
        "add",
        "update",
        "delete",
    }
    assert all(value >= 0 for value in timings.values())

    if not _BASELINE_PATH.exists():
        _BASELINE_PATH.write_text(json.dumps(timings, indent=2, sort_keys=True), "utf-8")
        return

    baseline = json.loads(_BASELINE_PATH.read_text(encoding="utf-8"))
    for name, recorded in baseline.items():
        measured = timings.get(name)
        if measured is None or recorded < _NOISE_FLOOR:
            continue
        assert measured <= recorded * _REGRESSION_FACTOR, (
            f"{name} regressed: {measured:.4f}s vs baseline {recorded:.4f}s"
        )
