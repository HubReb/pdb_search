"""Doc-currency gate test (constitution Principle I G3).

Reads README.md and CLAUDE.md and asserts that none of the forbidden legacy-stack
tokens appear. This is a mechanical, merge-blocking check.

Forbidden tokens (case-sensitive):
- Poetry
- psycopg2
- UserInteraction
- PsycopgDB

Any match is a build-failing defect, not deferred cleanup.
"""

from __future__ import annotations

from pathlib import Path

import pytest

# Forbidden legacy-stack tokens (constitution G3)
FORBIDDEN_TOKENS = ["Poetry", "psycopg2", "UserInteraction", "PsycopgDB"]

# Files to check
REPO_ROOT = Path(__file__).parent.parent
FILES_TO_CHECK = [
    REPO_ROOT / "README.md",
    REPO_ROOT / "CLAUDE.md",
]


@pytest.mark.parametrize("doc_path", FILES_TO_CHECK, ids=[f.name for f in FILES_TO_CHECK])
def test_no_forbidden_tokens(doc_path: Path) -> None:
    """Assert that doc_path contains none of the forbidden legacy-stack tokens.

    :param doc_path: path to the documentation file to check
    :type doc_path: Path
    """
    if not doc_path.exists():
        pytest.skip(f"{doc_path.name} not found — skipping doc-currency check")

    content = doc_path.read_text(encoding="utf-8")
    found_tokens = [token for token in FORBIDDEN_TOKENS if token in content]

    if found_tokens:
        lines_with_tokens = []
        for line_num, line in enumerate(content.splitlines(), start=1):
            for token in found_tokens:
                if token in line:
                    lines_with_tokens.append(f"  Line {line_num}: {line.strip()!r}")

        pytest.fail(
            f"{doc_path.name} contains forbidden legacy-stack tokens {found_tokens}.\n"
            "These tokens describe the superseded stack and must be removed before merge.\n"
            "Offending lines:\n" + "\n".join(lines_with_tokens)
        )
