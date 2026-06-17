"""US3 guard: the suite must not depend on developer-local database state.

A fresh checkout with no ``database.crypt``/``key`` must run the suite. This
test fails if any test module references those developer-local artifacts or the
deleted legacy ``ConfigReader``.
"""

from __future__ import annotations

from pathlib import Path

# Developer-local artifacts the suite must never reach for. A test creating its
# own temp encrypted INI is fine; reaching for the user's ``../../database.crypt``
# or the deleted legacy ``ConfigReader`` is not.
FORBIDDEN = ("../../database.crypt", "ConfigReader", "../../key")


def test_tests_have_no_local_state_dependencies() -> None:
    """No test module references developer-local credential artifacts."""
    tests_dir = Path(__file__).parent
    offenders: list[str] = []
    for module in tests_dir.rglob("test_*.py"):
        if module.name == Path(__file__).name:
            continue
        text = module.read_text(encoding="utf-8")
        for token in FORBIDDEN:
            if token in text:
                offenders.append(f"{module.name}: {token}")
    assert offenders == [], f"developer-local-state references found: {offenders}"
