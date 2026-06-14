"""Mechanical doc-currency gate — constitution Principle I, Gate G3.

After the legacy flat-layout modules are removed (T036), README.md and
CLAUDE.md MUST NOT contain any of the forbidden legacy-stack tokens.
This test is merge-blocking: any match is a build-failing defect.
"""

from __future__ import annotations

from pathlib import Path

import pytest

# The four forbidden tokens from constitution Principle I (doc-currency gate).
FORBIDDEN_TOKENS = ["Poetry", "psycopg2", "UserInteraction", "PsycopgDB"]

_REPO_ROOT = Path(__file__).parent.parent


@pytest.mark.parametrize("doc_file", ["README.md", "CLAUDE.md"])
@pytest.mark.parametrize("token", FORBIDDEN_TOKENS)
def test_doc_does_not_contain_legacy_token(doc_file: str, token: str) -> None:
    """Verify that the named doc file does not contain the forbidden legacy token.

    Args:
        doc_file: Relative path of the doc file from the repo root.
        token: The forbidden legacy-stack token (case-sensitive).
    """
    path = _REPO_ROOT / doc_file
    if not path.exists():
        pytest.skip(f"{doc_file} does not exist yet")
    content = path.read_text(encoding="utf-8")
    assert token not in content, (
        f"Doc-currency gate FAILED: {doc_file} contains forbidden token {token!r}. "
        f"Update the documentation to remove references to the superseded stack."
    )
