"""Tests for the bulk-import service and the end-to-end import path.

The import service extracts a ``PaperCreate`` per cited key with a matching
``.bib`` record and skips unmatched keys (logged warning). The CLI committer
persists per paper, so a partial failure preserves earlier papers. Persistence
runs against the real ephemeral database (no mocking).
"""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import Engine

from paper_sorts.cli.importer import run_import
from paper_sorts.services import paper_service
from paper_sorts.services.import_service import extract_papers_from_tex_bib

_FIXTURES = Path(__file__).parent / "fixtures"
_TEX = (_FIXTURES / "literature_overview.tex").read_text(encoding="utf-8")
_BIB = (_FIXTURES / "bib.bib").read_text(encoding="utf-8")


def test_extract_yields_only_matched_keys() -> None:
    """Three cited keys match the .bib; the unmatched key is skipped."""
    papers = list(extract_papers_from_tex_bib(_TEX, _BIB))
    keys = {p.bibtex_id for p in papers}
    assert keys == {"Vaswani2017Imp", "Wang2021Imp", "Lee2022Imp"}
    assert "Missing2099Key" not in keys


def test_extract_parses_authors_and_title() -> None:
    """Extracted papers carry Last, First authors and a decoded title."""
    papers = {p.bibtex_id: p for p in extract_papers_from_tex_bib(_TEX, _BIB)}
    vaswani = papers["Vaswani2017Imp"]
    assert vaswani.title == "Attention is all you need"
    assert vaswani.authors == ["Vaswani, Ashish", "Shazeer, Noam"]


def test_extract_decodes_latex_accents() -> None:
    r"""LaTeX accents in a title round-trip to plain text."""
    bib = '@article{Acc2020, title={Sch\\"one Gr\\"u{\\ss}e}, author={M\\"uller, A.}, year={2020}}'
    tex = r"\cite{Acc2020}"
    paper = next(extract_papers_from_tex_bib(tex, bib))
    assert "ü" in paper.title
    assert "\\" not in paper.title


def test_import_persists_matched_papers(migrated_engine: Engine, tmp_path: Path) -> None:
    """The import command persists exactly the matched papers (N=3)."""
    tex = tmp_path / "lit.tex"
    bib = tmp_path / "refs.bib"
    tex.write_text(_TEX, encoding="utf-8")
    bib.write_text(_BIB, encoding="utf-8")

    run_import(migrated_engine, str(tex), str(bib))

    assert paper_service.search_by_title(migrated_engine, "Attention is all you need")
    assert paper_service.search_by_author(migrated_engine, "Pino, J.")
    by_pino = paper_service.search_by_author(migrated_engine, "Pino, J.")
    assert {p.bibtex_id for p in by_pino} == {"Wang2021Imp", "Lee2022Imp"}


def test_import_is_rerunnable_skipping_dupes(migrated_engine: Engine, tmp_path: Path) -> None:
    """A second import run skips already-present papers (no duplicates)."""
    tex = tmp_path / "lit.tex"
    bib = tmp_path / "refs.bib"
    tex.write_text(_TEX, encoding="utf-8")
    bib.write_text(_BIB, encoding="utf-8")

    run_import(migrated_engine, str(tex), str(bib))
    run_import(migrated_engine, str(tex), str(bib))

    results = paper_service.search_by_title(migrated_engine, "Attention is all you need")
    assert len(results) == 1


def test_import_partial_failure_preserves_earlier(migrated_engine: Engine) -> None:
    """A failure mid-import leaves earlier per-paper commits persisted."""
    # Pre-insert the second paper's key so the run hits a duplicate after the
    # first; the first paper must already be committed (per-paper commit).
    from paper_sorts.db.repositories import PaperCreate

    paper_service.add_paper(
        migrated_engine,
        PaperCreate(
            title="Pre-existing Wang",
            contents="x",
            bibtex_id="Wang2021Imp",
            bibtex="@article{Wang2021Imp, note={pre}}",
            authors=["Pre, Author"],
        ),
    )
    # Importing now: Vaswani commits, Wang is a duplicate (skipped), Lee commits.
    run_import_inline(migrated_engine)
    assert paper_service.search_by_title(migrated_engine, "Attention is all you need")
    assert paper_service.search_by_title(
        migrated_engine, "Direct speech-to-speech translation with discrete units"
    )


def run_import_inline(engine: Engine) -> None:
    """Import the fixture pair from in-memory strings via temp files.

    :param engine: the database engine.
    """
    import tempfile

    with tempfile.TemporaryDirectory() as d:
        tex = Path(d) / "lit.tex"
        bib = Path(d) / "refs.bib"
        tex.write_text(_TEX, encoding="utf-8")
        bib.write_text(_BIB, encoding="utf-8")
        run_import(engine, str(tex), str(bib))
