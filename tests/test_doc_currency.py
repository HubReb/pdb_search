"""Mechanical doc-currency gate (constitution Principle I, Gate G3).

Once legacy flat-layout modules are removed, README.md and CLAUDE.md
MUST NOT contain any of the following forbidden legacy-stack tokens:
  - Poetry
  - psycopg2
  - UserInteraction
  - PsycopgDB

Any match is a merge-blocking test failure.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_PROJECT_ROOT = Path(__file__).parent.parent
_FORBIDDEN_TOKENS = ["Poetry", "psycopg2", "UserInteraction", "PsycopgDB"]
_DOC_FILES = ["README.md", "CLAUDE.md"]


@pytest.mark.parametrize("doc_file", _DOC_FILES)
@pytest.mark.parametrize("token", _FORBIDDEN_TOKENS)
def test_doc_currency(doc_file: str, token: str) -> None:
    """Assert that doc_file does not contain the forbidden legacy token.

    This is a case-sensitive search. Any match indicates the documentation
    still refers to the superseded stack.
    """
    doc_path = _PROJECT_ROOT / doc_file
    if not doc_path.exists():
        pytest.skip(f"{doc_file} does not exist")

    content = doc_path.read_text(encoding="utf-8")
    assert token not in content, (
        f"Forbidden legacy token {token!r} found in {doc_file}. "
        "Update the documentation to reference the modern stack "
        "(constitution Principle I, Gate G3)."
    )
