"""Doc-currency gate (constitution Principle I G3, merge-blocking).

After the legacy flat-layout modules are removed (FR-012), the migrated
user-facing documentation MUST NOT describe the superseded stack as if current.
``README.md`` and ``CLAUDE.md`` must contain none of the forbidden legacy tokens
(case-sensitive).
"""

from __future__ import annotations

from pathlib import Path

import pytest

FORBIDDEN_TOKENS = ("Poetry", "psycopg2", "UserInteraction", "PsycopgDB")
DOCS = ("README.md", "CLAUDE.md")

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("doc", DOCS)
def test_doc_has_no_forbidden_legacy_tokens(doc: str) -> None:
    """The migrated doc contains no forbidden legacy-stack token.

    :param doc: the documentation filename to scan.
    """
    text = (REPO_ROOT / doc).read_text(encoding="utf-8")
    found = [token for token in FORBIDDEN_TOKENS if token in text]
    assert found == [], f"{doc} contains forbidden legacy tokens: {found}"
