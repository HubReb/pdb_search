"""Doc-currency gate (G3): the user-facing docs must not name the legacy stack.

A case-sensitive check that ``README.md`` and ``CLAUDE.md`` contain none of the
forbidden legacy tokens, so the documentation stays current with the modern
stack and never silently re-introduces a stale reference.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_FORBIDDEN = ("Poetry", "psycopg2", "UserInteraction", "PsycopgDB")
_DOCS = ("README.md", "CLAUDE.md")
_ROOT = Path(__file__).resolve().parent.parent


@pytest.mark.parametrize("doc", _DOCS)
def test_doc_has_no_forbidden_tokens(doc: str) -> None:
    """The doc must not mention any legacy-stack token (case-sensitive)."""
    text = (_ROOT / doc).read_text(encoding="utf-8")
    present = [token for token in _FORBIDDEN if token in text]
    assert not present, f"{doc} still mentions legacy tokens: {present}"
