"""Baseline benchmark for paper_sorts interactive operations.

Constitution Principle IV (G2): This benchmark MUST NOT be permanently skipped.
Records baseline for: search_by_title, search_by_author, add_paper, update_field, delete_paper.

Run with:
    uv run pytest tests/benchmarks/ --benchmark-autosave

Results are saved to .benchmarks/ for comparison across runs.
"""

from __future__ import annotations

from paper_sorts.db.repositories import PaperCreate
from paper_sorts.services.paper_service import (
    add_paper,
    delete_paper,
    search_by_author,
    search_by_title,
    update_field,
)


def test_bench_search_by_title(benchmark: object, bench_db_url: str) -> None:
    """Benchmark: search_by_title (interactive baseline).

    :param benchmark: pytest-benchmark fixture
    :param bench_db_url: seeded ephemeral DB URL
    """
    result = benchmark(search_by_title, bench_db_url, "speech")  # type: ignore[operator]
    assert isinstance(result, list)


def test_bench_search_by_author(benchmark: object, bench_db_url: str) -> None:
    """Benchmark: search_by_author (interactive baseline).

    :param benchmark: pytest-benchmark fixture
    :param bench_db_url: seeded ephemeral DB URL
    """
    result = benchmark(search_by_author, bench_db_url, "Pino")  # type: ignore[operator]
    assert isinstance(result, list)


def test_bench_add_paper(benchmark: object, bench_db_url: str) -> None:
    """Benchmark: add_paper (interactive baseline).

    :param benchmark: pytest-benchmark fixture
    :param bench_db_url: seeded ephemeral DB URL
    """
    counter = {"n": 0}

    def _add() -> None:
        counter["n"] += 1
        key = f"BenchAdd{counter['n']}"
        data = PaperCreate(
            title=f"Benchmark Add Paper {counter['n']}",
            contents="Benchmark contents",
            bibtex_id=key,
            bibtex=f"@article{{{key}}}",
            authors=["Bench, Author"],
        )
        result = add_paper(bench_db_url, data)
        # Clean up immediately to avoid key conflicts on repeated runs
        delete_paper(bench_db_url, key)
        return result  # type: ignore[return-value]

    benchmark(_add)  # type: ignore[operator]


def test_bench_update_field(benchmark: object, bench_db_url: str) -> None:
    """Benchmark: update_field (interactive baseline, title update).

    :param benchmark: pytest-benchmark fixture
    :param bench_db_url: seeded ephemeral DB URL
    """
    # Use Smith2022Survey which is already seeded
    import itertools

    titles = itertools.cycle(["Bench Title A", "Bench Title B", "Bench Title C"])

    def _update() -> None:
        update_field(bench_db_url, "Smith2022Survey", "title", next(titles))

    benchmark(_update)  # type: ignore[operator]


def test_bench_delete_paper(benchmark: object, bench_db_url: str) -> None:
    """Benchmark: delete_paper (interactive baseline).

    :param benchmark: pytest-benchmark fixture
    :param bench_db_url: seeded ephemeral DB URL
    """
    counter = {"n": 0}

    def _delete_cycle() -> None:
        counter["n"] += 1
        key = f"BenchDel{counter['n']}"
        # Insert then delete
        add_paper(
            bench_db_url,
            PaperCreate(
                title=f"Bench Delete {counter['n']}",
                contents="...",
                bibtex_id=key,
                bibtex=f"@article{{{key}}}",
                authors=["Bench, Author"],
            ),
        )
        delete_paper(bench_db_url, key)

    benchmark(_delete_cycle)  # type: ignore[operator]
