"""Legacy UserInteraction tests — skipped post-modernization.

The UserInteraction class was replaced by the Typer CLI in
src/paper_sorts/cli/ as part of the 001-modernize-stack feature (FR-012).
CLI coverage is provided by tests/test_cli.py.
"""

import pytest

pytestmark = pytest.mark.skip(
    reason="Legacy placeholder for removed UserInteraction class. "
    "CLI is now tested in test_cli.py."
)


def test_legacy_placeholder() -> None:
    """Placeholder — superseded by test_cli.py."""
    pass
