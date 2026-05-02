"""Integration tests for the ``add`` flow (T033).

Service-level coverage of ``PaperService.add_paper`` (inline insert,
duplicate-key rejection, atomic rollback on partial failure) plus
CLI-helper coverage of ``cli.add._gather_input`` (the ``.bib``-file path
and the missing-file rejection — neither involves the database).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from paper_sorts.cli import add as add_cli
from paper_sorts.db.models import Author, BibEntry, Paper
from paper_sorts.db.repositories import DuplicateBibtexIdError, PaperCreate
from paper_sorts.services.paper_service import PaperService


def _payload(**overrides: object) -> PaperCreate:
    base = {
        "title": "T033 New Paper",
        "contents": "T033 summary",
        "bibtex_id": "T033Key",
        "bibtex": "@article{T033Key, author={New, A.}, title={T033 New Paper}, year={2026}}",
        "authors": ("New, A.", "Coauthor, B."),
    }
    base.update(overrides)
    return PaperCreate(**base)  # type: ignore[arg-type]


def test_add_inline_persists_paper_bib_authors_and_links(db_session: Session) -> None:
    service = PaperService(db_session)
    summary = service.add_paper(_payload())

    assert summary.bibtex_id == "T033Key"
    assert summary.title == "T033 New Paper"
    assert set(summary.authors) == {"New, A.", "Coauthor, B."}

    paper = db_session.execute(select(Paper).where(Paper.bibtex_id == "T033Key")).scalar_one()
    assert paper.title == "T033 New Paper"
    assert {a.name for a in paper.authors} == {"New, A.", "Coauthor, B."}

    bib = db_session.execute(select(BibEntry).where(BibEntry.bibtex_id == "T033Key")).scalar_one()
    assert bib.bibtex is not None
    assert "T033Key" in bib.bibtex


def test_add_duplicate_bibtex_id_raises_plain_language_error(
    db_session: Session,
) -> None:
    """``Wang2021LargeScaleSA`` is in the seed; re-adding the key is rejected."""
    service = PaperService(db_session)
    with pytest.raises(DuplicateBibtexIdError, match="Wang2021LargeScaleSA"):
        service.add_paper(
            _payload(
                bibtex_id="Wang2021LargeScaleSA",
                bibtex="@article{Wang2021LargeScaleSA, ... different source ...}",
            )
        )


def test_add_partial_failure_rolls_back_atomically(db_session: Session) -> None:
    """Bib UNIQUE on the source string fails the second insert; no orphans remain."""
    service = PaperService(db_session)
    shared_bibtex = "@article{Shared, author={X, Y.}, title={Same Source}, year={2026}}"
    service.add_paper(_payload(bibtex_id="A1", bibtex=shared_bibtex))

    with pytest.raises(IntegrityError):
        service.add_paper(
            _payload(
                bibtex_id="B1",
                bibtex=shared_bibtex,
                authors=("Solo, B.",),
            )
        )
    db_session.rollback()  # release the failed savepoint

    # Verify no orphan rows for the failed insert.
    paper_b = db_session.execute(select(Paper).where(Paper.bibtex_id == "B1")).scalar_one_or_none()
    assert paper_b is None

    bib_b = db_session.execute(
        select(BibEntry).where(BibEntry.bibtex_id == "B1")
    ).scalar_one_or_none()
    assert bib_b is None

    solo = db_session.execute(select(Author).where(Author.name == "Solo, B.")).scalar_one_or_none()
    assert solo is None


def test_gather_input_reads_bib_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    bib_content = "@article{FromFile2026, author={File and CoFile}, title={From File}, year={2026}}"
    bib_path = tmp_path / "x.bib"
    bib_path.write_text(bib_content)

    # The CSV splitter is the documented legacy quirk (split on ", "
    # exactly), so use single-token names to avoid the intra-name comma
    # collision; the quirk itself is preserved per FR-002.
    answers = iter(["File, CoFile", "From File", "FromFile2026", "summary text"])
    monkeypatch.setattr(add_cli, "ask_text", lambda *_a, **_kw: next(answers))

    payload = add_cli._gather_input(bib_file=bib_path, summary=None)
    assert payload is not None
    assert payload.bibtex == bib_content
    assert payload.bibtex_id == "FromFile2026"
    assert payload.authors == ("File", "CoFile")


def test_gather_input_missing_bib_file_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    answers = iter(["A, B.", "Title", "Key", "summary"])
    monkeypatch.setattr(add_cli, "ask_text", lambda *_a, **_kw: next(answers))

    payload = add_cli._gather_input(bib_file=tmp_path / "nonexistent.bib", summary=None)
    assert payload is None
