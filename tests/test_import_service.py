"""Bulk-import tests: matched entries inserted, unmatched skipped, idempotent."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import Engine

from paper_sorts.cli.importer import run_import
from paper_sorts.services import paper_service
from paper_sorts.services.import_service import extract_papers_from_tex_bib
from tests.fixtures.seed_papers import SEED_BIB, SEED_TEX


def test_extract_yields_only_matched_entries() -> None:
    papers = list(extract_papers_from_tex_bib(SEED_TEX, SEED_BIB))
    keys = {p.bibtex_id for p in papers}
    # Lee2021Direct matches the .bib; NoSuchKey2099 has no record and is skipped.
    assert "Lee2021Direct" in keys
    assert "NoSuchKey2099" not in keys


def test_extract_populates_authors_and_summary() -> None:
    papers = {p.bibtex_id: p for p in extract_papers_from_tex_bib(SEED_TEX, SEED_BIB)}
    lee = papers["Lee2021Direct"]
    assert "Lee, Ann" in lee.authors
    assert lee.summary  # the following non-empty line became the summary


def test_run_import_inserts_and_is_idempotent(engine: Engine, tmp_path: Path) -> None:
    tex = tmp_path / "overview.tex"
    bib = tmp_path / "refs.bib"
    tex.write_text(SEED_TEX, encoding="utf-8")
    bib.write_text(SEED_BIB, encoding="utf-8")

    inserted = run_import(engine, str(tex), str(bib))
    assert inserted == 1
    assert paper_service.search_by_author(engine, "Lee, Ann")[0].bibtex_id == "Lee2021Direct"

    # Re-run: the existing BibTeX key is skipped, no duplicates.
    again = run_import(engine, str(tex), str(bib))
    assert again == 0
    assert len(paper_service.search_by_author(engine, "Lee, Ann")) == 1
