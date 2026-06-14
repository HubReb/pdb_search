"""Legacy DatabaseConnector tests — skipped post-modernization.

The original paper_sorts.database_connector and paper_sorts.config_reader
modules were removed as part of the 001-modernize-stack feature (FR-012).
Their functionality is now covered by:
    - tests/test_repositories.py (persistence layer)
    - tests/test_services.py (service layer)
    - tests/test_cli.py (CLI integration)

These tests are kept as a historical reference only; they cannot run because
the modules they import no longer exist.
"""

import pytest

pytestmark = pytest.mark.skip(
    reason="Legacy tests for removed modules (FR-012). Superseded by "
    "test_repositories.py, test_services.py, and test_cli.py."
)


def test_legacy_placeholder() -> None:
    """Placeholder — real tests have been migrated to the modern test suite."""
    pass
