"""Mechanical doc-currency gate (constitution Principle I).

After the legacy flat-layout modules are removed, README.md and CLAUDE.md
MUST NOT contain any of the forbidden legacy-stack tokens:
  - Poetry
  - psycopg2
  - UserInteraction
  - PsycopgDB

This test is a build-failing gate — a match is a defect, not deferred cleanup.
"""

from __future__ import annotations

from pathlib import Path

import pytest

# Forbidden tokens per constitution Principle I (doc-currency gate)
FORBIDDEN_TOKENS = ["Poetry", "psycopg2", "UserInteraction", "PsycopgDB"]

# Files to check
_REPO_ROOT = Path(__file__).resolve().parent.parent
_FILES_TO_CHECK = [
    _REPO_ROOT / "README.md",
    _REPO_ROOT / "CLAUDE.md",
]


@pytest.mark.parametrize("filepath", _FILES_TO_CHECK, ids=lambda p: p.name)
def test_no_forbidden_legacy_tokens(filepath: Path) -> None:
    """Verify that filepath does not contain any forbidden legacy-stack tokens.

    :param filepath: Path to the documentation file to check.
    """
    if not filepath.exists():
        pytest.skip(f"{filepath.name} does not exist yet")

    content = filepath.read_text(encoding="utf-8")
    violations = [token for token in FORBIDDEN_TOKENS if token in content]

    assert not violations, (
        f"{filepath.name} contains forbidden legacy-stack tokens: {violations}. "
        "These tokens must be removed as part of FR-016 / constitution Principle I "
        "doc-currency gate."
    )
