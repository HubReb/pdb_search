"""Benchmark baseline tests for paper-sorts.

These tests are currently skipped pending a modern benchmark rewrite (T046).
The baseline JSON records the legacy implementation's performance on a
personal-library-sized dataset.  Once T046 implements proper benchmarking,
this file should be updated with measurement tooling and the skip removed.
"""

from __future__ import annotations

import pytest


@pytest.mark.skip(reason="Awaiting T046: modern benchmark rewrite against measured baseline")
def test_search_by_title_baseline() -> None:
    """Placeholder: benchmark search_by_title against the recorded baseline."""
    raise NotImplementedError("Benchmark not yet implemented — see T046")


@pytest.mark.skip(reason="Awaiting T046: modern benchmark rewrite against measured baseline")
def test_search_by_author_baseline() -> None:
    """Placeholder: benchmark search_by_author against the recorded baseline."""
    raise NotImplementedError("Benchmark not yet implemented — see T046")
