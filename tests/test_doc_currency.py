"""Doc-currency gate (G3): forbidden legacy-stack tokens must not appear.

Once the legacy flat-layout modules are removed (FR-012), the migrated
user-facing docs must not describe the superseded stack as if current. This is a
case-sensitive search over ``README.md`` and ``CLAUDE.md``; any match is a
build-failing defect (constitution Principle I).
"""

from __future__ import annotations

from pathlib import Path

import pytest

_FORBIDDEN = ["Poetry", "psycopg2", "UserInteraction", "PsycopgDB"]
_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("doc", ["README.md", "CLAUDE.md"])
def test_doc_has_no_forbidden_tokens(doc: str) -> None:
    text = (_ROOT / doc).read_text(encoding="utf-8")
    found = [token for token in _FORBIDDEN if token in text]
    assert not found, f"{doc} contains forbidden legacy-stack tokens: {found}"
