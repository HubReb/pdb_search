"""Doc-currency gate test (Principle I, constitution v1.3.0).

Once the legacy flat-layout modules are removed (FR-012), README.md and CLAUDE.md
MUST NOT contain any of these forbidden legacy-stack tokens (case-sensitive):
  - Poetry
  - psycopg2
  - UserInteraction
  - PsycopgDB

This test is mechanical and merge-blocking per constitution Principle I.
"""

import pathlib

import pytest

REPO_ROOT = pathlib.Path(__file__).parent.parent
FORBIDDEN_TOKENS = ["Poetry", "psycopg2", "UserInteraction", "PsycopgDB"]
FILES_TO_CHECK = ["README.md", "CLAUDE.md"]


@pytest.mark.parametrize("filename", FILES_TO_CHECK)
@pytest.mark.parametrize("token", FORBIDDEN_TOKENS)
def test_doc_currency_no_forbidden_token(filename: str, token: str) -> None:
    """Verify that filename does not contain the forbidden legacy-stack token.

    :param filename: Relative path to the documentation file to check.
    :param token: Forbidden token string (case-sensitive).
    """
    filepath = REPO_ROOT / filename
    assert filepath.exists(), f"{filename} does not exist at {filepath}"
    content = filepath.read_text(encoding="utf-8")
    assert token not in content, (
        f"Doc-currency violation: {filename!r} contains forbidden token {token!r}. "
        f"Remove or replace all references to the superseded legacy stack. "
        f"(Constitution Principle I, merge-blocking gate)"
    )
