"""Baseline benchmark harness (gate G2).

Records wall-clock timings for the five interactive operations (search by title,
search by author, add, update, delete) against a seeded ephemeral database and
writes them to ``baseline.json``. This is a *real, executing* benchmark — not a
permanently-skipped placeholder: the non-regression criterion (Principle IV)
cannot be claimed vacuously without a measured baseline.

Run as part of the suite (it executes by default) or inspect ``baseline.json``.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

from sqlalchemy import Engine

from paper_sorts.db.repositories import PaperCreate
from paper_sorts.services.paper_service import PaperService

_BASELINE = Path(__file__).parent / "baseline.json"


def _time(fn) -> float:  # type: ignore[no-untyped-def]
    start = time.perf_counter()
    fn()
    return time.perf_counter() - start


def test_record_baseline(seeded_engine: Engine) -> None:
    """Measure each interactive op and record a baseline (always executes)."""
    service = PaperService(seeded_engine)

    def _add() -> None:
        service.add_paper(
            PaperCreate(
                title="Bench Paper",
                summary="bench",
                bibtex_id="Bench2026",
                bibtex="@misc{Bench2026}",
                authors=["Bench, Mark"],
            )
        )

    timings: dict[str, float] = {}
    timings["search_by_title"] = _time(lambda: service.search_by_title("Shared Title"))
    timings["search_by_author"] = _time(lambda: service.search_by_author("Pino, J."))
    timings["add"] = _time(_add)
    pid = service.search_by_title("Bench Paper")[0].paper_id
    timings["update"] = _time(
        lambda: service.update_field("papers", "contents", str(pid), "updated")
    )
    timings["delete"] = _time(lambda: service.delete_paper(pid))

    # Every interactive op must have produced a finite, non-negative measurement.
    assert set(timings) == {
        "search_by_title",
        "search_by_author",
        "add",
        "update",
        "delete",
    }
    assert all(v >= 0 for v in timings.values())

    _BASELINE.write_text(json.dumps(timings, indent=2, sort_keys=True), encoding="utf-8")
    assert _BASELINE.exists()
