"""Benchmark-specific fixtures (reuse the session-wide ephemeral PG)."""

from __future__ import annotations

# The ephemeral DB fixtures live in the top-level tests/conftest.py and are
# inherited here; nothing extra is needed, but this file marks the directory as
# a benchmark package with its own conftest scope.
