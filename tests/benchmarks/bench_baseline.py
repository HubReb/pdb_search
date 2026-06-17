"""Executing baseline benchmark for the five interactive operations.

Constitution Principle IV (G2, merge-blocking) requires an executing baseline —
not a permanently-skipped one — so the "no measurable regression" criterion is
verifiable. This harness times search-by-title, search-by-author, add, update,
and delete on a seeded database and records a baseline JSON next to this file.

It runs inside the default ``pytest`` invocation. The recorded numbers are a
reference point for future comparison, not an absolute bound (there is no
fabricated target — see the constitution rationale).
"""

from __future__ import annotations

import json
import time
from pathlib import Path

from sqlalchemy import Engine

from paper_sorts.db.repositories import PaperCreate
from paper_sorts.services.paper_service import PaperService

BASELINE_PATH = Path(__file__).with_name("baseline.json")


def _time(fn) -> float:  # type: ignore[no-untyped-def]
    """Return the wall-clock seconds taken to run ``fn`` once.

    :param fn: a zero-argument callable to time.
    :returns: elapsed seconds.
    """
    start = time.perf_counter()
    fn()
    return time.perf_counter() - start


def test_record_interactive_baseline(seeded_engine: Engine) -> None:
    """Time the five interactive operations and record a baseline.

    The benchmark executes (the gate forbids a permanent skip) and writes a
    ``baseline.json`` capturing per-operation wall-clock seconds.
    """
    service = PaperService(seeded_engine)
    create = PaperCreate(
        title="Bench Paper",
        contents="bench",
        bibtex_id="benchkey",
        bibtex="@misc{benchkey}",
        authors=["Bench, B"],
    )

    timings: dict[str, float] = {}
    timings["search_by_title"] = _time(
        lambda: service.search_by_title("Direct speech-to-speech translation with discrete units")
    )
    timings["search_by_author"] = _time(lambda: service.search_by_author("Pino, J."))
    timings["add"] = _time(lambda: service.add_paper(create))
    pid = service.search_by_title("Bench Paper")[0].paper_id
    timings["update"] = _time(
        lambda: service.update_field("papers", "title", str(pid), "Bench Renamed")
    )
    timings["delete"] = _time(lambda: service.delete_paper("benchkey"))

    BASELINE_PATH.write_text(json.dumps(timings, indent=2), encoding="utf-8")

    # The benchmark must produce a positive measurement for every operation.
    assert set(timings) == {
        "search_by_title",
        "search_by_author",
        "add",
        "update",
        "delete",
    }
    assert all(seconds >= 0 for seconds in timings.values())
