"""SC-006 baseline timing benchmark for the legacy implementation.

Records per-operation wall-clock timings for the legacy
``paper_sorts/run.py`` + ``DatabaseConnector`` stack against a freshly
seeded ephemeral PostgreSQL. Numbers landed by ``--baseline-record``
(T008) become the reference for ``--baseline-compare`` in T046.

Operation surface (5 ops, per ``spec.md`` SC-006):

* search-by-title       -- ``DatabaseConnector.search_by_title`` +
                           ``search_for_bibtex_entry_by_id`` in-process
* search-by-author      -- ``DatabaseConnector.search_by_author`` +
                           ``search_for_entry_by_specified_paper_information``
                           + ``search_for_bibtex_entry_by_id`` in-process
* single add (inline)   -- ``python -m paper_sorts.run`` subprocess
* single update (title) -- ``python -m paper_sorts.run`` subprocess
* single delete         -- ``DatabaseConnector.delete_paper_entry_from_database``
                            in-process

Three of five ops are timed in-process. ``delete`` is direct because the
legacy interactive CLI's top-level menu has no delete affordance. The
two ``search`` ops are also direct because the legacy CLI's search
journey crashes on
``IndexError: list index out of range`` inside
``helpers.pretty_print_results`` (it indexes ``bibtex_data[1]`` but
``DatabaseConnector.search_for_bibtex_entry_by_id`` returns a *list* of
rows, not the unpacked tuple). The in-process timing covers the same DB
round-trips the CLI would have driven, minus the broken print step.
T046 mirrors the same asymmetry on the modern side (search via
``PaperService.search_by_*`` in-process; add + update via the
``pdbsearch`` subprocess; delete via ``PaperService.delete_paper``).

Timer window: from the moment the operation's stdin tokens are written
until the next top-level menu marker (``What do you want to do?``)
appears on stdout. This excludes Python interpreter startup and the
initial DB-connection dance, includes the operation work + result
rendering + return-to-menu.

Host hardware (recorded at ``--baseline-record`` time): see
``baseline.json`` ``host`` field. The legacy stack's per-operation
timing is dominated by per-call ``psycopg2.connect()`` (PsycopgDB opens
and closes a fresh connection inside every ``store_in_db`` /
``fetch_from_db`` call).
"""

from __future__ import annotations

import json
import logging
import os
import platform
import select
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest


def _read_until(proc: subprocess.Popen[bytes], marker: bytes, timeout: float) -> bytes:
    """Read proc.stdout until marker is in the running buffer; raise on timeout/EOF."""
    deadline = time.monotonic() + timeout
    buf = bytearray()
    assert proc.stdout is not None
    fd = proc.stdout.fileno()
    while marker not in buf:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"timed out waiting for {marker!r}; got {bytes(buf)!r}")
        ready, _, _ = select.select([fd], [], [], remaining)
        if not ready:
            continue
        chunk = os.read(fd, 4096)
        if not chunk:
            raise RuntimeError(
                f"legacy CLI closed stdout before emitting {marker!r}; got {bytes(buf)!r}"
            )
        buf.extend(chunk)
    return bytes(buf)


def _spawn_legacy(env: dict[str, Any]) -> subprocess.Popen[bytes]:
    cmd = [
        sys.executable,
        "-u",
        "-m",
        "paper_sorts.run",
        "-c",
        str(env["config_path"]),
        "-k",
        str(env["key_path"]),
    ]
    # The subprocess runs in tmp_path (so legacy log files stay scoped),
    # but the legacy paper_sorts/ lives at the repo root, so PYTHONPATH
    # has to include it.
    proc_env = {
        **os.environ,
        "PYTHONPATH": str(Path(__file__).resolve().parents[2]),
    }
    return subprocess.Popen(  # noqa: S603 — cmd is a fixed Python invocation, no shell, no user input
        cmd,
        cwd=str(env["tmp_path"]),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=0,
        env=proc_env,
    )


def _time_op(env: dict[str, Any], payload: bytes) -> float:
    """Time one operation: write payload, wait for return-to-top-menu marker."""
    proc = _spawn_legacy(env)
    try:
        _read_until(proc, b"What do you want to do?", 30.0)
        assert proc.stdin is not None
        start = time.perf_counter()
        proc.stdin.write(payload)
        proc.stdin.flush()
        _read_until(proc, b"What do you want to do?", 30.0)
        elapsed = time.perf_counter() - start
    finally:
        try:
            assert proc.stdin is not None
            proc.stdin.write(b"q\n")
            proc.stdin.flush()
        except (BrokenPipeError, OSError):
            pass
        try:
            proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
    return elapsed


def _make_connector(env: dict[str, Any], logger_name: str) -> Any:
    from paper_sorts.database_connector import DatabaseConnector

    return DatabaseConnector(
        env["db_config"],
        logging.WARNING,
        logger_name=logger_name,
        log_file=str(env["tmp_path"] / f"{logger_name}.log"),
    )


def _time_search_by_title(env: dict[str, Any], title: str) -> float:
    """Time the DB calls a successful CLI search-by-title would have driven."""
    connector = _make_connector(env, "bench_search_title")
    start = time.perf_counter()
    papers = connector.search_by_title(title)
    chosen = papers[0]
    connector.search_for_bibtex_entry_by_id(chosen)
    return time.perf_counter() - start


def _time_search_by_author(env: dict[str, Any], author: str) -> float:
    """Time the DB calls a successful CLI search-by-author would have driven."""
    connector = _make_connector(env, "bench_search_author")
    start = time.perf_counter()
    papers = connector.search_by_author(author)
    chosen = papers[0]
    paper = connector.search_for_entry_by_specified_paper_information(chosen)
    connector.search_for_bibtex_entry_by_id(paper)
    return time.perf_counter() - start


def _time_delete(env: dict[str, Any], paper: dict[str, Any]) -> float:
    """Time DatabaseConnector.delete_paper_entry_from_database in-process."""
    connector = _make_connector(env, "bench_delete")
    start = time.perf_counter()
    connector.delete_paper_entry_from_database(
        paper["bibtex"],
        paper["authors"],
        paper["bibtex_id"],
        paper["title"],
        paper["contents"],
    )
    return time.perf_counter() - start


def _fetch_added_paper_id(env: dict[str, Any], bibtex_id: str) -> int:
    import psycopg2

    with psycopg2.connect(**env["db_config"]) as conn, conn.cursor() as cur:
        cur.execute("SELECT id FROM papers WHERE bibtex_id = %s", (bibtex_id,))
        row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"benchmark add did not insert {bibtex_id!r}")
    return int(row[0])


@pytest.mark.skip(reason="legacy stack removed in T026; T046 will rewrite for modern")
@pytest.mark.benchmark
def test_baseline(
    legacy_db_env: dict[str, Any],
    baseline_record: bool,
    baseline_compare: bool,
) -> None:
    """Run the 5 SC-006 operations; record or compare per-op wall-clock."""
    timings: dict[str, float] = {}

    # Searches run in-process: the legacy CLI's pretty_print_results
    # crashes on bibtex_data[1] (see module docstring).
    timings["search_by_title"] = _time_search_by_title(
        legacy_db_env,
        "Large-scale Self- and Semi-Supervised learning for speech translation",
    )
    timings["search_by_author"] = _time_search_by_author(legacy_db_env, "Schöttler, K.")

    # add (inline bibtex): top "2" -> authors -> title -> bibkey -> "2" (no
    # bib file) -> bibtex -> summary.
    bench_bibkey = "BenchPaper2026"
    bench_title = "Bench Paper Title"
    bench_authors = "Bench, A."
    bench_bibtex = (
        "@inproceedings{BenchPaper2026,author={Bench, A.},title={Bench Paper Title},year={2026}}"
    )
    bench_summary = "Synthetic benchmark insertion."
    add_payload = (
        b"2\n"
        + bench_authors.encode()
        + b"\n"
        + bench_title.encode()
        + b"\n"
        + bench_bibkey.encode()
        + b"\n"
        + b"2\n"
        + bench_bibtex.encode()
        + b"\n"
        + bench_summary.encode()
        + b"\n"
    )
    timings["add_inline"] = _time_op(legacy_db_env, add_payload)

    new_paper_id = _fetch_added_paper_id(legacy_db_env, bench_bibkey)

    # update title: top "3" -> table "papers" (the menu accepts the number "1"
    # too, but the legacy code forwards the raw input to update_entry whose
    # dispatch only knows canonical names; "1" silently fails) -> column "1"
    # (title; this one IS normalised) -> id -> new title -> confirm "1".
    updated_title = "Updated Bench Paper Title"
    update_payload = (
        b"3\npapers\n1\n"
        + str(new_paper_id).encode()
        + b"\n"
        + updated_title.encode()
        + b"\n"
        + b"1\n"
    )
    timings["update_title"] = _time_op(legacy_db_env, update_payload)

    timings["delete"] = _time_delete(
        legacy_db_env,
        {
            "bibtex": bench_bibtex,
            "authors": [bench_authors],
            "bibtex_id": bench_bibkey,
            "title": updated_title,
            "contents": bench_summary,
        },
    )

    baseline_path = Path(__file__).parent / "baseline.json"

    if baseline_record:
        record = {
            "host": {
                "machine": platform.machine(),
                "system": platform.system(),
                "release": platform.release(),
                "python": sys.version.split()[0],
            },
            "implementation": "legacy",
            "ops": timings,
        }
        baseline_path.write_text(json.dumps(record, indent=2) + "\n")
        return

    if baseline_compare:
        if not baseline_path.exists():
            pytest.fail(
                f"--baseline-compare requested but {baseline_path} not found; "
                "run --baseline-record first."
            )
        baseline = json.loads(baseline_path.read_text())
        # T046 will choose the tolerance with informed eyes; for now flag any
        # op that took more than 1.5x its baseline.
        tolerance = 1.5
        regressions = [
            (op, baseline["ops"][op], elapsed)
            for op, elapsed in timings.items()
            if op in baseline["ops"] and elapsed > baseline["ops"][op] * tolerance
        ]
        if regressions:
            lines = "\n".join(
                f"  {op}: {ref:.3f}s -> {now:.3f}s ({(now / ref - 1) * 100:+.1f}%)"
                for op, ref, now in regressions
            )
            pytest.fail(f"SC-006 regression detected:\n{lines}")
        return

    # Smoke mode (no flag): assert all timings are positive.
    assert all(t > 0 for t in timings.values()), timings
