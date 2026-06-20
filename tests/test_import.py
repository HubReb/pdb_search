"""Real-DB tests for the bulk-import path."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import Engine

from paper_sorts.cli.importer import run_import
from paper_sorts.services.paper_service import PaperService

_FIXTURES = Path(__file__).parent / "fixtures"
_TEX = _FIXTURES / "literature_overview.tex"
_BIB = _FIXTURES / "sample.bib"


def test_import_inserts_matching_entries(migrated_engine: Engine) -> None:
    service = PaperService(migrated_engine)
    inserted = run_import(service, _TEX, _BIB)
    assert inserted == 2
    assert service.search_by_title("Direct speech-to-speech translation with discrete units")
    assert service.search_by_author("Pino, Juan")


def test_import_skips_unmatched_key(migrated_engine: Engine) -> None:
    service = PaperService(migrated_engine)
    run_import(service, _TEX, _BIB)
    # The .tex cites Missing2099NoMatch, which has no .bib record -> skipped.
    assert service.search_by_title("A paper with no matching bib entry") == []


def test_import_rerun_does_not_duplicate(migrated_engine: Engine) -> None:
    service = PaperService(migrated_engine)
    run_import(service, _TEX, _BIB)
    second = run_import(service, _TEX, _BIB)
    assert second == 0  # all already present -> all skipped
    assert len(service.search_by_author("Pino, Juan")) == 2
