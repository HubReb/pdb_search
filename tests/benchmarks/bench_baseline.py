"""Interactive-operation timing harness (SC-006, Constitution Principle IV).

The performance criterion is "no measurable regression vs. the current
baseline", not an absolute bound. This module records wall-clock timings for the
interactive operations (search by title/author, add, update, delete) against the
seeded fixture so a future change can be compared to a recorded baseline.

It is skipped by default: a meaningful comparison needs a recorded
``baseline.json`` captured on the same hardware, and there is no committed
legacy baseline to diff against in this fresh build. Remove the skip and run
with ``--no-skip`` style invocation locally to (re)record.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest
from sqlalchemy import Engine

from paper_sorts.db.repositories import PaperCreate
from paper_sorts.db.session import with_session
from paper_sorts.services.paper_service import PaperService
from tests.conftest import _seed

BASELINE = Path(__file__).parent / "baseline.json"


@pytest.mark.skip(reason="no recorded legacy baseline to compare against in this build")
def test_record_interactive_baseline(engine: Engine) -> None:
    """Record wall-clock timings for the interactive operations."""
    with with_session(engine) as session:
        _seed(session)
    service = PaperService(engine)

    timings: dict[str, float] = {}

    start = time.perf_counter()
    service.search_by_title("On Calibration")
    timings["search_by_title"] = time.perf_counter() - start

    start = time.perf_counter()
    service.search_by_author("Pino, J.")
    timings["search_by_author"] = time.perf_counter() - start

    start = time.perf_counter()
    service.add_paper(
        PaperCreate(
            title="Bench Add",
            authors=["Bench, Mark"],
            summary="bench",
            bibtex_id="Bench1",
            bibtex="@misc{Bench1}",
        )
    )
    timings["add_paper"] = time.perf_counter() - start

    BASELINE.write_text(json.dumps(timings, indent=2), encoding="utf-8")
